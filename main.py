import requests
from bs4 import BeautifulSoup
import csv
import time
import logging
import openai

openai.api_key = "token openai"
#Глубина поиска по сайту
DEEP = 3

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)

BASE_URL = "https://belabraziv.ru"
HEADERS = {"User-Agent": "Mozilla/5.0"}
visited_urls = set()



def get_page_content(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            logging.info(f"Успешно получен контент: {url}")
            return response.text
        else:
            logging.warning(f"Неожиданный статус код {response.status_code} для {url}")
    except requests.RequestException as e:
        logging.error(f"Ошибка запроса {url}: {e}")
    return None

def extract_links(html, parent_url):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if href.startswith("/"):
            href = BASE_URL + href  # Преобразуем относительные ссылки в абсолютные
        if href.startswith(BASE_URL) and href.count("/") <= parent_url.count("/") + 2:
            links.add(href)
    logging.info(f"Найдено {len(links)} ссылок на {parent_url}")
    return links

def extract_title(soup):
    title = soup.title.string if soup.title else "Без заголовка"
    return title.strip() if title else "Без заголовка"

def extract_main_text(soup):
    for script in soup(["script", "style", "nav", "footer", "header", "aside"]):
        script.extract()
    text = " ".join(soup.stripped_strings)
    return text[:4096]  # Ограничение длины текста

def get_summary(text):
    if not text.strip():
        logging.warning("Пропущена суммаризация: пустой текст")
        return "Нет содержимого для суммаризации"
    try:
        prompt = f"Summarize: {text}"
        response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": f"Тебе будет передан весь текст с сайта. Твоя задача - очистить текст от мусора(тех инфо, повторы и тд), структурировать и выдать ответв формате текста"}, {"role": "user", "content": f"{text}"}]
        	)
        logging.info("Суммаризация выполнена успешно")
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"Ошибка суммаризации: {e}")
    return "Ошибка суммаризации"

def save_to_csv_row(url, title, summary, filename="results.csv"):
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([url, title, summary])
    logging.info(f"Данные сохранены для {url}")

def crawl(url, depth=0):
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
    summary = get_summary(main_text)
    save_to_csv_row(url, title, summary)
    if depth < 2:
        links = extract_links(html, url)
        for link in links:
            time.sleep(1)
            crawl(link, depth + 1)

def init_csv(filename="results.csv"):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Title", "Summary"])
    logging.info(f"Создан новый файл {filename}")

if __name__ == "__main__":
    logging.info("Начало работы краулера")
    init_csv()
    crawl(BASE_URL)
    logging.info("Завершение работы краулера")