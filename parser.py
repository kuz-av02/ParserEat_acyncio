import concurrent

from bs4 import BeautifulSoup
from urllib.parse import urljoin
from aiohttp.client import ClientTimeout
import time
import requests
import json
import os
import asyncio
import aiohttp


# парсер инструкции
def parseInstruction(soup):
    try:
        names_texts = [element.get_text() for element in soup.find_all('span', class_='emotion-1dvddtv')]
        return [el.replace('\xa0', ' ') for el in names_texts]
    except:
        print()


# парсер БЖУ
def parseEnergyValue(soup):
    try:
        names_texts = [element.get_text() for element in soup.find_all('span', class_='emotion-k2zivt')]
        count_texts = [element.get_text() for element in soup.find_all('div', class_='emotion-8fp9e2')]

        return {element: count_texts[index] for index, element in enumerate(names_texts)}
    except:
        print()


# парсер рецепта
def parseRecipes(soup):
    try:
        Ing_texts = [element.get_text() for element in soup.find_all('span', class_='emotion-mdupit')]
        countIng_texts = [element.get_text() for element in soup.find_all('span', class_='emotion-bsdd3p')]

        return {element: countIng_texts[index] for index, element in enumerate(Ing_texts)}
    except:
        print()


# сохранение результата в файл
def save(result, file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    else:
        existing_data = {}

    # Объединение словарей
    existing_data.update(result)

    # Запись обновленного словаря в файл JSON
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=4)


# перебор всех нижних категорий рецептов и возврат словаря с ссылками на категорию и кол-вом страниц в ней
def parseListSubCategoriesRecipes(link, soup):
    countList = soup.find_all('span', class_='emotion-19jfb4z')
    nameList = soup.find_all('span', class_='emotion-e9xsk4')
    resDict = {}
    for index, element in enumerate(nameList):
        parent_a = element.find_parent('a')
        if parent_a:
            link = urljoin(link, parent_a.get('href'))
            name = element.get_text()[:-len(countList[index].get_text())]
            pagesCount = round(int(countList[index].get_text()) / 14)
            resDict[name] = [link, pagesCount]
    return resDict


# проверка статуса сайта по ссылке и возрат html верстки при статусе 200
async def fetch_url(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            return await response.text()
        else:
            print('Ошибка при загрузке страницы:', response.status)
            return ''


closedLink = {}

async def parseListOfRecipes(session, url, sem):
    async with sem:
        html_content = await fetch_url(session, url)
        # Извлечение нужных данных из HTML-кода страницы с рецептом
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            reseptsList = soup.find_all('span', class_='emotion-1pdj9vu')
            porsiyList = soup.find_all('span', class_='emotion-tqfyce')
            resDict = {}
            # mb while sem > 0??
            for index, element in enumerate(reseptsList):
                try:
                    parent_a = element.find_parent('a')
                    if parent_a:
                        link = parent_a.get('href')
                        name = element.get_text()
                        link_for_rules = urljoin(url, link)
                        new_html_content = await fetch_url(session, link_for_rules)
                        if new_html_content:
                            newSoup = BeautifulSoup(new_html_content, 'html.parser')
                            resDict[name] = {
                                'Ингредиенты': parseRecipes(newSoup),
                                'БЖУ': parseEnergyValue(newSoup),
                                'Инструкция': parseInstruction(newSoup),
                                'кол-во порций': porsiyList[index].get_text()
                            }
                except Exception as e:
                    if link_for_rules:
                        closedLink[link_for_rules] = element.get_text()
                    else:
                        closedLink[url] = element.get_text()
                    print(element.get_text(), e)
            save(resDict, 'output.json')


async def main(links):
    max_session = 50
    # Создаем семафор для ограничения количества сессий
    session_semaphore = asyncio.Semaphore(max_session)
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=600)) as session:
        tasks = [asyncio.create_task(parseListOfRecipes(session, link, session_semaphore)) for link in links]
        print(f'Всего ссылок: {len(tasks)}')
        executor = concurrent.futures.ThreadPoolExecutor(20)
        # Устанавливаем ThreadPoolExecutor в качестве исполнителя по умолчанию для asyncio
        asyncio.get_event_loop().set_default_executor(executor)
        await asyncio.gather(*tasks)
        executor.shutdown(wait=True)

if __name__ == '__main__':
    cur_time = time.time()
    url = 'https://eda.ru/recepty'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    re_dict = {}
    re_dict['main'] = [url, 714]  # 714
    re_dict.update(parseListSubCategoriesRecipes(url, soup))
    links = []
    for key, value in re_dict.items():
        for index in range(1, value[1] + 1):
            links.append(f'{value[0]}?page={index}')

    try:
        asyncio.run(main(links[:200]))
        save(closedLink, 'closed.json')
    except Exception as e:
        print(e)
    print(f"Затраченное на работу скрипта время: {time.time() - cur_time}")

