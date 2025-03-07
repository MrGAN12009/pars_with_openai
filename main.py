import requests
from bs4 import BeautifulSoup
import csv
import time
import logging
import openai
import PyPDF2
import io

openai.api_key = "token openai"

# Глубина поиска
DEEP = 3

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("crawler.log"), logging.StreamHandler()],
)

BASE_URL = "https://belabraziv.ru"
HEADERS = {"User-Agent": "Mozilla/5.0"}
visited_urls = set()

# Типы файлов, которые будем анализировать
FILE_EXTENSIONS = [".txt", ".csv", ".pdf"]


def get_page_content(url):
    """Загружает HTML страницы."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            logging.info(f"Успешно загружен контент: {url}")
            return response.text
        else:
            logging.warning(f"Ошибка {response.status_code} для {url}")
    except requests.RequestException as e:
        logging.error(f"Ошибка запроса {url}: {e}")
    return None


def extract_links(html, parent_url):
    """Извлекает ссылки из страницы."""
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    file_links = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if href.startswith("/"):
            href = BASE_URL + href  # Преобразуем относительные ссылки

        # Если это ссылка на файл — запоминаем отдельно
        if any(href.lower().endswith(ext) for ext in FILE_EXTENSIONS):
            file_links.add(href)
        elif href.startswith(BASE_URL) and href.count("/") <= parent_url.count("/") + 2:
            links.add(href)

    logging.info(f"Найдено {len(links)} страниц и {len(file_links)} файлов на {parent_url}")
    return links, file_links


def download_file(url):
    """Скачивает файл и извлекает текст (если поддерживается)."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "")

            if url.endswith(".txt") or "text/plain" in content_type:
                return response.text[:5000]  # Ограничение по длине

            elif url.endswith(".csv") or "text/csv" in content_type:
                return "\n".join(response.text.split("\n")[:50])  # Первые 50 строк

            elif url.endswith(".pdf") or "application/pdf" in content_type:
                return extract_text_from_pdf(response.content)

        logging.warning(f"Не удалось скачать {url}")
    except requests.RequestException as e:
        logging.error(f"Ошибка загрузки файла {url}: {e}")
    return None


def extract_text_from_pdf(pdf_content):
    """Извлекает текст из PDF-файла."""
    try:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
        text = "\n".join(page.extract_text() for page in reader.pages if page.extract_text())
        return text[:5000]  # Ограничение по длине
    except Exception as e:
        logging.error(f"Ошибка чтения PDF: {e}")
    return None


def extract_title(soup):
    """Извлекает заголовок страницы."""
    return (soup.title.string or "Без заголовка").strip()


def extract_main_text(soup):
    """Извлекает основной текст страницы."""
    for script in soup(["script", "style", "nav", "footer", "header", "aside"]):
        script.extract()
    return " ".join(soup.stripped_strings)[:4096]  # Ограничение по длине


def get_summary(text, file_data):
    """Запрашивает суммаризацию у GPT, включая информацию о файлах."""
    if not text.strip() and not file_data:
        logging.warning("Пропущена суммаризация: пустой контент")
        return "Нет данных для суммаризации"

    file_info = "\n".join(f"{name}: {content[:500]}" for name, content in file_data.items())

    prompt = f"Тебе будет передан текст с сайта и файлы. Очисти и структурируй:\n\nТекст:\n{text}\n\nФайлы:\n{file_info}"

    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": "Структурируй и очищай текст."},
                      {"role": "user", "content": prompt}]
        )
        logging.info("Суммаризация выполнена")
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"Ошибка суммаризации: {e}")
    return "Ошибка суммаризации"


def save_to_csv_row(url, title, summary, filename="results.csv"):
    """Сохраняет результаты в CSV."""
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([url, title, summary])
    logging.info(f"Сохранено: {url}")


def crawl(url, depth=0):
    """Рекурсивный обход сайта."""
    if depth > DEEP or url in visited_urls:
        return

    logging.info(f"Сканирую: {url} (Глубина: {depth})")
    html = get_page_content(url)
    if not html:
        return

    visited_urls.add(url)
    soup = BeautifulSoup(html, "html.parser")

    title = extract_title(soup)
    main_text = extract_main_text(soup)

    # Извлекаем ссылки на файлы и скачиваем их
    _, file_links = extract_links(html, url)
    file_data = {file_url: download_file(file_url) for file_url in file_links}

    # Убираем пустые файлы
    file_data = {k: v for k, v in file_data.items() if v}

    summary = get_summary(main_text, file_data)
    save_to_csv_row(url, title, summary)

    # Дальше идем только на обычные страницы
    if depth < 2:
        page_links, _ = extract_links(html, url)
        for link in page_links:
            time.sleep(1)
            crawl(link, depth + 1)


def init_csv(filename="results.csv"):
    """Создает CSV-файл."""
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Title", "Summary"])
    logging.info(f"Создан новый файл {filename}")


if __name__ == "__main__":
    logging.info("Запуск краулера")
    init_csv()
    crawl(BASE_URL)
    logging.info("Завершение работы краулера")
