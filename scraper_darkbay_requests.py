# AnilKumarRavuru

import time
import datetime
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from mysql_cryptomarketsdb import MySQLcryptomarketsDB
from selenium_networksetting import *
import requests
from bs4 import BeautifulSoup
import concurrent.futures


# ++++++++++++++++++++ You need to update the following 3 variables ++++++++++++++++++++
# Student Name Abbr
g_sStudentNameAbbr = "al"  # You need to update this
# Your Username and Password for YellowBrick Market
g_sMarketUserName = "OrionStars"  # You need to update this
g_sMarketPassword = "sandiego"  # You need to update this
g_nScrapingFreqDaysProductDesc = 30  # days
g_nScrapingFreqDaysProductRating = 7  # days
g_nScrapingFreqDaysVendorProfile = 30  # days
g_nScrapingFreqDaysVendorRating = 7  # days
# market information
g_sMarketNameAbbr = "db"
# You need to provide the latest market URL by looking up the list in https://dark.fail
g_sMarketURL = "http://darkbayupenqdqvv.onion/"
g_nMarketGlobalID = 26
g_sOutputDirectoryTemp = "/home/rob/Covid19/UndergroundMarkets/db/"
# time to wait for human to input CAPTCHA
g_nTimeSecToWait = 30 * 24 * 60 * 60  # 30 days
# set it True if you want to use the default username and password
g_bUseDefaultUsernamePasswd = False
g_nBatchSize = 100  # Number of rows to update at once.

# Default proxies for secure http browsing for tor
proxies = {'http': 'socks5h://localhost:9050', 'https': 'socks5h://localhost:9050'}

# Choose one of the categories. If you want to scrape new category, please look at the html source code
c1 = 'category/02164500-b00f-11e9-8b8a-6907ed514d60'    # Carded Items
c2 = 'category/183960e0-b010-11e9-aefb-77eff3685e85'    # Security & Hosting
c3 = 'category/395aad00-b00e-11e9-ad2a-f1b9db0180aa'    # Counterfeit Items
c4 = 'category/a0f5ca30-b00f-11e9-b247-abf90d62ea65'    # Services
c5 = 'category/aa3983c0-b00e-11e9-8e2a-c3ca29964ed4'    # Digital Products
c6 = 'category/c0c5d3f0-b009-11e9-aea4-712111355e3c'    # Fraud
c7 = 'category/de55f160-b00f-11e9-b35e-33a425dd7b01'    # Other Listings
c8 = 'category/dff98750-b00e-11e9-b8fd-3190ad2a874f'    # Jewles & Gold
c9 = 'category/ec94d1a0-b009-11e9-b912-d148462df001'    # Drugs
c10 = 'category/ed6c2ca0-b00f-11e9-9397-b98faaa936e4'   # Software & Malware
c11 = 'category/f4120b60-b00d-11e9-8e5c-632a53ce2a83'   # Guides & Tutorials

g_startIndexes = {c1: 1, c2: 1, c3: 1, c4: 1, c5: 1, c6: 1, c7: 1, c8: 1, c9: 1, c10: 1, c11: 1}


def Login(aBrowserDriver):
    aBrowserDriver.get(g_sMarketURL)
    aWaitNextPage = WebDriverWait(aBrowserDriver, g_nTimeSecToWait)  # Wait up to x seconds (30 days).
    aWaitNextPage.until(EC.element_to_be_clickable((By.XPATH,
                                                    "//a[contains(@href,'http://darkbayupenqdqvv.onion/signup')]")))
    # fill the username and password
    aInputElementUsername = aBrowserDriver.find_element_by_xpath("//input[contains(@name,'username')]")
    aInputElementUsername.send_keys(g_sMarketUserName)
    aInputElementPassword = aBrowserDriver.find_element_by_xpath("//input[contains(@name,'password')]")
    aInputElementPassword.send_keys(g_sMarketPassword)
    aInputElementCaptcha = aBrowserDriver.find_element_by_xpath("//input[contains(@name,'captcha')]")
    aInputElementCaptcha.send_keys('')
    # Waits till you login
    aWaitNextPage.until(
        EC.element_to_be_clickable((By.XPATH, "//a[contains(@href,'http://darkbayupenqdqvv.onion/profile/cart')]")))


# Scrapes a link, returns product ids and vendor ids in it.
def ScrapeNewProductsAndVendorsByPageLink(pageLink):
    pageContent = BeautifulSoup(loginSession.get(pageLink, proxies=proxies).content, 'html.parser')
    productDescLinks = list(map(lambda x: x.select('a')[0]['href'], pageContent.find_all('div', class_='col-md-8')))
    productIds = list(map(lambda x: x[x.rfind('/') + 1:], productDescLinks))
    vendorDescLinks = list(set(map(lambda x: x.select('a')[0]['href'], pageContent.find_all('div', class_='col-md-5'))))
    vendorIds = list(map(lambda x: x[x.rfind('/') + 1:], vendorDescLinks))
    print('.')
    return productIds, vendorIds


# Given product id, scrapes product description, returns its information
def ScrapePDsForProductId(productId):
    nProductGlobalId, sProductId = productId[0], productId[1]
    productDescPageLink = g_sMarketURL + '/product/' + sProductId
    sPDContent = loginSession.get(productDescPageLink, proxies=proxies).text
    sCurrentUTCTime = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    sLocalOutputFileName = "{}_{}_{}_pd".format(sCurrentUTCTime, g_nMarketGlobalID, sProductId)
    sLocalOutputFileNameFullPath = g_sOutputDirectoryTemp + 'pd/' + sLocalOutputFileName
    fp = open(sLocalOutputFileNameFullPath, 'w')
    fp.write(sPDContent)
    fp.close()
    print('Scraped', sLocalOutputFileName)
    productDescriptionInfo = {
        'product_global_ID': nProductGlobalId,
        'product_market_ID': sProductId,
        'scraping_time_pd': sCurrentUTCTime,
        'product_desc_file_path_in_FS': sLocalOutputFileNameFullPath
    }
    return productDescriptionInfo


# Given vendor id, scrapes vendor profile, returns its information
def ScrapeVPsForVendorId(vendorId):
    nVendorGlobalId, sVendorId = vendorId[0], vendorId[1]
    vendorProfileLink = g_sMarketURL + '/vendor/' + sVendorId
    sVPContent = loginSession.get(vendorProfileLink, proxies=proxies).text
    sCurrentUTCTime = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    sLocalOutputFileName = "{}_{}_{}_vp".format(sCurrentUTCTime, g_nMarketGlobalID, sVendorId)
    sLocalOutputFileNameFullPath = g_sOutputDirectoryTemp + 'vp/' + sLocalOutputFileName
    fp = open(sLocalOutputFileNameFullPath, 'w')
    fp.write(sVPContent)
    fp.close()
    print('Scraped', sLocalOutputFileName)
    VendorProfileInfo = {
        'vendor_global_ID': nVendorGlobalId,
        'vendor_market_ID': sVendorId,
        'scraping_time_vp': sCurrentUTCTime,
        'vendor_profile_file_path_in_FS': sLocalOutputFileNameFullPath
    }
    return VendorProfileInfo


if __name__ == '__main__':
    aMySQLcrptmktDB = MySQLcryptomarketsDB()
    aMySQLcrptmktDB.m_sStudentNameAbbr = g_sStudentNameAbbr
    aMySQLcrptmktDB.m_sMarketNameAbbr = g_sMarketNameAbbr
    aMySQLcrptmktDB.m_nScrapingFreqDaysProductDesc = g_nScrapingFreqDaysProductDesc
    aMySQLcrptmktDB.m_nScrapingFreqDaysProductRating = g_nScrapingFreqDaysProductRating
    aMySQLcrptmktDB.m_nScrapingFreqDaysVendorProfile = g_nScrapingFreqDaysVendorProfile
    aMySQLcrptmktDB.m_nScrapingFreqDaysVendorRating = g_nScrapingFreqDaysVendorRating
    aMySQLcrptmktDB.MySQLQueryBasicInfor()
    g_nMarketGlobalID = aMySQLcrptmktDB.m_nMarketGlobalID
    if g_bUseDefaultUsernamePasswd:
        g_sMarketUserName = aMySQLcrptmktDB.m_sMarketUserName
        g_sMarketPassword = aMySQLcrptmktDB.m_sMarketPassword
    aBrowserDriver = selenium_setup_firefox_network()
    Login(aBrowserDriver)

    # Pass cookies from selenium to url requests.
    # In future we can save these in db and use them until they expire.
    loginSession = requests.session()
    for cookie in aBrowserDriver.get_cookies():
        loginSession.cookies.update({cookie['name']: cookie['value']})

    # Add New Products to product_list and vendors to vendors_list
    for cat_code in g_startIndexes:
        categoryLink = g_sMarketURL + cat_code
        catContent = loginSession.get(categoryLink, proxies=proxies).content
        paginationDiv = list(map(lambda x: x.text,
                                 BeautifulSoup(catContent, 'html.parser').find_all('li', class_='page-item')))
        pageIndices = list(map(int, list(filter(lambda x: x.isdigit(), paginationDiv))))
        pageCount = max(pageIndices)
        startPageIndex = g_startIndexes[cat_code]
        print('Total Number of Pages: ', pageCount)
        # Scrape max 100 pages at once
        for batchStart in range(startPageIndex, pageCount+1, 100):
            startTime = time.time()
            batchEnd = min(batchStart+100, pageCount+1)

            # ###### To implement Parallelism in future, Scraping pages in each category is implemented in a function.
            #
            # allProductIds, allVendorsIds = [], []
            # for pageIndex in range(batchStart, batchEnd):
            #     sOnePageLink = categoryLink + '?page=' + str(pageIndex)
            #     productsIds, vendorsIds = ScrapeNewProductsAndVendorsByPageLink(sOnePageLink)
            #     allProductIds += productsIds
            #     allVendorsIds += vendorsIds
            #
            # ###### Parallelism will end here hopefully.

            # ############### Parallel Implementation ###############
            pageLinks = list(map(lambda x: categoryLink + '?page=' + str(x), range(batchStart, batchEnd)))
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                lProductVendorIdsGen = executor.map(ScrapeNewProductsAndVendorsByPageLink, pageLinks)
            allProductIdSet, allVendorsIdSet = set(), set()
            for lPV in lProductVendorIdsGen:
                for productId in lPV[0]:
                    allProductIdSet.add(productId)
                for vendorId in lPV[1]:
                    allVendorsIdSet.add(vendorId)
            allProductIds, allVendorsIds = list(allProductIdSet), list(allVendorsIdSet)
            # #######################################################

            productsInfo = None
            if len(allProductIds) > 0:
                productsInfo = {
                    'productIds': allProductIds,
                    'product_category': cat_code
                }
            vendorsInfo = None
            if len(allVendorsIds) > 0:
                vendorsInfo = {
                    'vendorIds': allVendorsIds
                }
            nProductsAdded, nVendorsAdded = aMySQLcrptmktDB.AddProductsAndVendorsIfNotExists(productsInfo, vendorsInfo)
            endTime = time.time()
            print('Batch Time for {} Pages: {}'.format(batchEnd - batchStart, endTime - startTime))
            print('Products Added:', nProductsAdded)
            print('Vendors Added:', nVendorsAdded)
    print('Added all New products and Vendors.')

    # ScrapeProductDescriptions
    # Get a batch of product ids to scrape, scrape them, get next batch.
    print('Scraping Product Descriptions...')
    nBatchCount = 1
    productIdsToScrapePD = aMySQLcrptmktDB.GetBatchOfProductsToScrape(g_nScrapingFreqDaysProductDesc, g_nBatchSize)
    while len(productIdsToScrapePD) > 0:
        print('Batch :', nBatchCount)
        nBatchCount += 1
        # To implement Parallelism in future, Scraping each PD is implemented in a function.
        productsDescInfo = []
        for proIndex in range(len(productIdsToScrapePD)):
            productsDescInfo.append(ScrapePDsForProductId(productIdsToScrapePD[proIndex]))
        # Parallelism will end here hopefully.
        aMySQLcrptmktDB.UpdateBatchOfProductDescriptions(productsDescInfo)
        # Get New Batch of Product Ids To scrape.
        productIdsToScrapePD = aMySQLcrptmktDB.GetBatchOfProductsToScrape(g_nScrapingFreqDaysProductDesc, g_nBatchSize)
    print('Scraping Product Descriptions Done.')

    # ScrapeVendorDescriptions
    # Get a batch of vendor ids to scrape, scrape them, get next batch.
    print('Scraping Vendor Profiles...')
    nBatchCount = 1
    vendorIdsToScrapeVP = aMySQLcrptmktDB.GetBatchOfVendorsToScrape(g_nScrapingFreqDaysVendorProfile, g_nBatchSize)
    while len(vendorIdsToScrapeVP) > 0:
        print('Batch :', nBatchCount)
        nBatchCount += 1
        # To implement Parallelism in future, Scraping each VP is implemented in a function.
        vendorsProfileInfo = []
        for vendorIndex in range(len(vendorIdsToScrapeVP)):
            vendorsProfileInfo.append(ScrapeVPsForVendorId(vendorIdsToScrapeVP[vendorIndex]))
        # Parallelism will end here hopefully.
        aMySQLcrptmktDB.UpdateBatchOfVendorProfiles(vendorsProfileInfo)
        # Get New Batch of Vendor Ids To scrape.
        vendorIdsToScrapeVP = aMySQLcrptmktDB.GetBatchOfVendorsToScrape(g_nScrapingFreqDaysVendorProfile, g_nBatchSize)
        break
    print('Scraping Vendor Profiles Done.')

    # ScrapeProductRatings

    # ScrapeVendorRatings
