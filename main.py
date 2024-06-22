import asyncio
import json
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from kafka import KafkaProducer

SBR_WS_CDP = 'wss://brd-customer-hl_4c27d997-zone-realestatebrowser:vv96awqb9m2q@brd.superproxy.io:9222'
BASE_URL='https://www.emlakjet.com/'
LOCATION='istanbul'



async def run(pw , producer):
    print('Connecting to Scraping Browser...')
    browser = await pw.chromium.connect_over_cdp(SBR_WS_CDP)
    try:
        page = await browser.new_page()
        print(f'Connected! Navigating to {BASE_URL}')
        await page.goto(BASE_URL)
        

        #enter istanbul and press enter
        await page.fill('input[class="fTNyyb"]', LOCATION )
        await page.keyboard.press('Enter')

        print('Waiting for page results...')
        await page.wait_for_load_state('load')


        content = await page.inner_html('div[id="listing-search-wrapper"]')

        soup = BeautifulSoup(content, 'html.parser')

        for idx , div in enumerate(soup.find_all('div' , class_ ='_3qUI9q')):

            data = {}

            #FİYATLAR
            price = div.find('p' , class_ = "_2C5UCT")
            if price:
                spans = price.find_all('span')
                if len(spans) >= 2:
                    price = spans[0].text
                    fiyat = price.split(' ')[0]
                    para_birimi= price.split(' ')[1]     
                        
            #BAŞLIKLAR
            title = div.find('div' , class_ = '_1TNSG2')
            title= title.find('h3').text #title
            
            #KONUMLAR
            location = div.find('div' , class_ = '_2wVG12').find('span').text
            il= location.split('-')[0].strip() # il
            ilce = location.split('-')[1].strip() #ilce
            mahalle= location.split('-')[2].strip() #mahalle
            
            #ETİKETLER
            ad_prop = div.find('div' , class_ = '_3nT075')
            ozellik_listesi=ad_prop.find_all('div')
            for ozellik in ozellik_listesi:
                if 'ÖNE ÇIKAN' in ozellik:
                    one_cikan = True #one_cikan
                else:
                    one_cikan= False #one_cikan
                
                if 'YENİ' in ozellik:
                    yeni = True #yeni
                else:
                    yeni= False #yeni

                if 'JETFIRSAT'  in ozellik:
                    jet_firsat= True #jet_firsat
                else:
                    jet_firsat= False #jet_firsat
            

            #BOYUTLAR
            size_data = div.find('div', class_='_2UELHn').find_all('span')
            
            for size in size_data:
                if size.find('i', class_='material-icons'):
                    icon = size.find('i', class_='material-icons').text.strip()
                    text = size.get_text(separator=' ', strip=True)
                    cleaned_text = text.replace(icon, '').strip()

                    if icon == 'home':
                        home = cleaned_text
                        if home =='Villa':
                            floor ='1. Kat'
                    elif icon == 'weekend':
                        rooms = cleaned_text
                    elif icon == 'texture':
                        square_meter = cleaned_text
                    elif icon == 'event':
                        last_update = cleaned_text
                    elif icon == 'layers':
                        floor = cleaned_text


            

            data.update({
                'title':title,
                'il':il,
                'ilce':ilce,
                'mahalle':mahalle,
                'one_cikan':one_cikan,
                'yeni':yeni,
                'jet_firsat':jet_firsat,
                'price':fiyat,
                'currency':para_birimi,
                'home':home,
                'floor':floor,
                'rooms':rooms,
                'square_meter':square_meter,
                'last_update':last_update
            })

            print("sending data to producer")
            producer.send("properties", value=json.dumps(data).encode("utf-8"))
            print("data sent to kafka")

            
            
            




        print('Navigated! Scraping page content...')
        
    finally:
        await browser.close()


async def main():
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"],max_clock_ms=5000)
    async with async_playwright() as playwright:
        await run(playwright , producer)


if __name__ == '__main__':
    asyncio.run(main())