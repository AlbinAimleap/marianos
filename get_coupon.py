import json
import pandas as pd
import aiohttp
import asyncio
import logging
from pathlib import Path
from config import Config
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def get_json_data(session, endpoint, params, headers=None):
    headers = headers or Config.HEADERS
    async with session.get(endpoint, params=params, headers=headers) as response:
        data = await response.json()
        return data['data']['coupons']

async def get_all_coupons():
    all_data = []
    offset = 0
    page_size = 72
    base_url = 'https://www.marianos.com/atlas/v1/savings-coupons/v1/coupons'
    
    logger.info("Starting to fetch all coupons")
    async with aiohttp.ClientSession() as session:
        tasks = []
        while True:
            params = {
                'filter.sort': 'relevance',
                'filter.onlyNewCoupons': 'false',
                'page.size': str(page_size),
                'filter.status': 'unclipped',
                'page.offset': str(offset),
                'projections': 'coupons.compact',
            }
            
            task = asyncio.create_task(get_json_data(session, base_url, params))
            tasks.append(task)
            offset += page_size
            
            if len(tasks) >= 10:  # Process in batches of 10
                results = await asyncio.gather(*tasks)
                for coupons in results:
                    if not coupons:
                        break
                    all_data.extend([{
                        'krogerCouponNumber': coupon['krogerCouponNumber'],
                        'shortDescription': coupon['shortDescription']
                    } for coupon in coupons])
                tasks = []
                if not coupons:
                    break
                
        if tasks:  # Process remaining tasks
            results = await asyncio.gather(*tasks)
            for coupons in results:
                if not coupons:
                    break
                all_data.extend([{
                    'krogerCouponNumber': coupon['krogerCouponNumber'],
                    'shortDescription': coupon['shortDescription']
                } for coupon in coupons])
                
    logger.info(f"Total coupons fetched: {len(all_data)}")
    return all_data

async def get_coupon_details(session, kroger_coupon_number):
    headers = Config.HEADERS.copy()
    headers.update({
        'x-facility-id': '53100526',
        'x-modality': '{"type":"PICKUP","locationId":"53100526"}',
        'x-modality-type': 'PICKUP',
    })
    
    params = {
        'filter.krogerCouponNumber': str(kroger_coupon_number),
        'filter.type': 'standard',
        'projections': 'coupons.full',
    }
    
    base_url = 'https://www.marianos.com/atlas/v1/savings-coupons/v1/coupons'
    coupons = await get_json_data(session, base_url, params, headers)
    return coupons[0]

async def process_coupon_batch(batch, session):
    return await asyncio.gather(*[get_coupon_details(session, coupon['krogerCouponNumber']) 
                                for coupon in batch])

async def process_coupons():
    all_coupons = await get_all_coupons()
    data_list = []
    batch_size = 50
    
    logger.info("Starting to process coupon details")
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(0, len(all_coupons), batch_size):
            batch = all_coupons[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}, coupons {i} to {i + len(batch)}")
            tasks.append(process_coupon_batch(batch, session))
        
        results = await asyncio.gather(*tasks)
        for coupon_results in results:
            for coupon_data in coupon_results:
                coupon_info = {
                    'digital_coupon_description': coupon_data['shortDescription']
                }
                
                for upc in coupon_data['upcs']:
                    data_list.append({
                        'upc': upc,
                        **coupon_info
                    })
            
            logger.info(f"Total UPCs processed so far: {len(data_list)}")
    
    logger.info(f"Completed processing all coupons. Total UPCs: {len(data_list)}")
    return data_list

async def get_coupons(crawl=True):
    output_filename = Path(f"marianos_coupons_{datetime.now().date()}.json")
    if not crawl and output_filename.exists():
        return output_filename
    
    if output_filename.exists():
        output_filename.unlink()
    
    result = await process_coupons()
    with open(output_filename, 'w') as f:
        json.dump(result, f, indent=4)
    return output_filename

if __name__ == "__main__":
    asyncio.run(get_coupons())