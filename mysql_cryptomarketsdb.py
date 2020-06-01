# The program will scrape the ads page by page
# The URLs of ads will be written into the MariaDB database
# in the server "jaguar.cs.gsu.edu"

# Run "$ sudo apt-get install sshpass" in Ubuntu terminal if you see relevant errors.

import datetime
import os
import mysql
import mysql.connector


class MySQLcryptomarketsDB:
    # Student information
    m_sStudentNameAbbr = ""
    m_sStudentIDinDB = ""
    # The .onion link address of the Dream Market
    m_sMarketNameAbbr = ""
    m_sMarketURL = ""
    m_nMarketGlobalID = 0
    m_sMarketUserName = ""
    m_sMarketPassword = ""
    # remote SQL database server
    m_sServerHost = "jaguar.cs.gsu.edu"
    m_sServerUser = "covid19"
    m_sServerPasswd = "uvNjdEbsn3t5uyiQkXgw"
    m_sServerDatabaseUser = "covid19_scraper"
    m_sServerDatabasePasswd = "jmfu8Q4W8v2F8ABzO7EU"
    m_sServerDatabase = "underground_markets_covid19"
    m_sServerPort = "3306"
    m_bServerSQLBuffered = True
    # remote root directory in the file system of the server OS
    m_sRemoteRootDirectoryProductDesc = ""
    m_sRemoteRootDirectoryProductRating = ""
    m_sRemoteRootDirectoryVendorProfile = ""
    m_sRemoteRootDirectoryVendorRating = ""
    # scraping frequency for 4 kinds of web pages
    m_nScrapingFreqDaysProductDesc = 0
    m_nScrapingFreqDaysProductRating = 0
    m_nScrapingFreqDaysVendorProfile = 0
    m_nScrapingFreqDaysVendorRating = 0
    # vendor and product global IDs
    m_nProductGlobalID = 0
    m_nVendorGlobalID = 0
    # scraping time
    m_sCurrentUTCTime = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")

    def MySQLQueryBasicInfor(self):
        # query basic information of the mysql database
        aMariaDB_cryptomarkets = mysql.connector.connect(host=self.m_sServerHost, user=self.m_sServerDatabaseUser,
                                                         passwd=self.m_sServerDatabasePasswd,
                                                         database=self.m_sServerDatabase, port=self.m_sServerPort,
                                                         buffered=self.m_bServerSQLBuffered)
        aCursorDB_cryptomarkets = aMariaDB_cryptomarkets.cursor(dictionary=True)
        # Fetch student information
        aSelectStmt_students_list = "SELECT student_ID FROM students_list WHERE student_name_abbr='" + \
                                    self.m_sStudentNameAbbr + "';"
        aCursorDB_cryptomarkets.execute(aSelectStmt_students_list)
        if aCursorDB_cryptomarkets.rowcount == 1:
            aOneStudentRecord = aCursorDB_cryptomarkets.fetchone()
            self.m_sStudentIDinDB = aOneStudentRecord["student_ID"]
        # Fetch market information
        aSelectStmt_cryptomarkets_list = "SELECT * FROM cryptomarkets_list WHERE cryptomarket_name_abbr='" + \
                                         self.m_sMarketNameAbbr + "';"
        aCursorDB_cryptomarkets.execute(aSelectStmt_cryptomarkets_list)
        if aCursorDB_cryptomarkets.rowcount == 1:
            aOneMarketRecord = aCursorDB_cryptomarkets.fetchone()
            self.m_sMarketURL = aOneMarketRecord["cryptomarket_url"]
            self.m_nMarketGlobalID = aOneMarketRecord["cryptomarket_global_ID"]
            self.m_sMarketUserName = aOneMarketRecord["my_username"]
            self.m_sMarketPassword = aOneMarketRecord["my_password"]
            self.m_sRemoteRootDirectoryProductDesc = aOneMarketRecord["product_desc_root_directory"]
            self.m_sRemoteRootDirectoryProductRating = aOneMarketRecord["product_rating_root_directory"]
            self.m_sRemoteRootDirectoryVendorProfile = aOneMarketRecord["vendor_profile_root_directory"]
            self.m_sRemoteRootDirectoryVendorRating = aOneMarketRecord["vendor_rating_root_directory"]
        aMariaDB_cryptomarkets.close()

    # Given product ids, vendor ids, Add only if they don't exist in product_list and vendor_list. Else Ignore.
    def AddProductsAndVendorsIfNotExists(self, sProductsInfo=None, sVendorsInfo=None):
        aMariaDB_cryptomarkets = mysql.connector.connect(host=self.m_sServerHost, user=self.m_sServerDatabaseUser,
                                                         passwd=self.m_sServerDatabasePasswd,
                                                         database=self.m_sServerDatabase,
                                                         port=self.m_sServerPort, buffered=self.m_bServerSQLBuffered)
        aCursorDB_cryptomarkets = aMariaDB_cryptomarkets.cursor()
        pCount = 0
        if sProductsInfo:
            # Fast but complex, INSERT (SELECT IF LEFT JOIN IS NULL => not Exists)
            productValues = list(map(lambda x: (self.m_nMarketGlobalID, x, '0'*14, 0, '0'*14, 0,
                                                sProductsInfo['product_category']), sProductsInfo['productIds']))
            productQuery = "INSERT INTO product_list (cryptomarket_global_ID, product_market_ID, " \
                           "last_scraping_time_pr, my_lock_pr, last_scraping_time_pd, my_lock_pd, product_category)" \
                           "WITH temp(cryptomarket_global_ID, product_market_ID, last_scraping_time_pr, my_lock_pr," \
                           "last_scraping_time_pd, my_lock_pd, product_category) AS (VALUES {}) " \
                           "SELECT temp.cryptomarket_global_ID, temp.product_market_ID, temp.last_scraping_time_pr," \
                           "temp.my_lock_pr, temp.last_scraping_time_pd, temp.my_lock_pd, " \
                           "temp.product_category FROM temp LEFT JOIN product_list ON " \
                           "product_list.cryptomarket_global_ID = temp.cryptomarket_global_ID AND " \
                           "product_list.product_market_ID = temp.product_market_ID WHERE " \
                           "product_list.product_market_ID IS NULL".format(str(tuple(productValues))[1:-1])
            aCursorDB_cryptomarkets.execute(productQuery)

            # easy approach, slow, INSERT ON DUPLICATE KEY UPDATE, failing, need to change values and try
            """
            productQuery = "INSERT INTO product_list (cryptomarket_global_ID, product_market_ID, " \
                           "last_scraping_time_pr, my_lock_pr, last_scraping_time_pd, my_lock_pd, product_category) " \
                           "VALUES ({}, %s, {}, {}, {}, {}, %s) ON DUPLICATE KEY UPDATE " \
                           "product_global_ID = product_global_ID".format(self.m_nMarketGlobalID, '0'*14, 0, '0'*14, 0)
            productValues = list(map(lambda x: (sProductsInfo['productIds'], sProductsInfo['product_category']),
                                     sProductsInfo))
            start_time = time.time()
            aCursorDB_cryptomarkets.executemany(productQuery, productValues)
            """

            pCount = aCursorDB_cryptomarkets.rowcount
            aMariaDB_cryptomarkets.commit()
        vCount = 0
        if sVendorsInfo:
            # Fast but complex, INSERT (SELECT IF LEFT JOIN IS NULL => not Exists)
            vendorValues = list(map(lambda x: (self.m_nMarketGlobalID, x, '0'*14, 0, '0'*14, 0),
                                    sVendorsInfo['vendorIds']))
            vendorQuery = "INSERT INTO vendor_list (cryptomarket_global_ID, vendor_market_ID, " \
                          "last_scraping_time_vr, my_lock_vr, last_scraping_time_vp, my_lock_vp) " \
                          "WITH temp(cryptomarket_global_ID, vendor_market_ID, last_scraping_time_vr, my_lock_vr, " \
                          "last_scraping_time_vp, my_lock_vp) AS (VALUES {}) " \
                          "SELECT temp.cryptomarket_global_ID, temp.vendor_market_ID, temp.last_scraping_time_vr, " \
                          "temp.my_lock_vr, temp.last_scraping_time_vp, temp.my_lock_vp " \
                          "FROM temp LEFT JOIN vendor_list ON " \
                          "vendor_list.cryptomarket_global_ID = temp.cryptomarket_global_ID AND " \
                          "vendor_list.vendor_market_ID = temp.vendor_market_ID WHERE " \
                          "vendor_list.vendor_market_ID IS NULL".format(str(tuple(vendorValues))[1:-1])
            aCursorDB_cryptomarkets.execute(vendorQuery)

            # easy approach, slow, INSERT ON DUPLICATE KEY UPDATE, failing, need to change values and try
            """
            vendorQuery = "INSERT INTO vendor_list (cryptomarket_global_ID, vendor_market_ID, " \
                          "last_scraping_time_vr, my_lock_vr, last_scraping_time_vp, my_lock_vp) " \
                          "VALUES ({}, %s, {}, {}, {}, {}) ON DUPLICATE KEY UPDATE " \
                          "vendor_global_ID = vendor_global_ID".format(self.m_nMarketGlobalID, '0' * 14, 0, '0' * 14, 0)
            vendorValues = list(map(lambda x: (sVendorsInfo['productIds']), sVendorsInfo))
            aCursorDB_cryptomarkets.executemany(vendorQuery, vendorValues)
            """

            vCount = aCursorDB_cryptomarkets.rowcount
            aMariaDB_cryptomarkets.commit()
        aCursorDB_cryptomarkets.close()
        aMariaDB_cryptomarkets.close()
        return pCount, vCount

    # Given cutoff days and batch size, return product ids to scrape.
    def GetBatchOfProductsToScrape(self, pdFrequencyDays, nBatchSize):
        aMariaDB_cryptomarkets = mysql.connector.connect(host=self.m_sServerHost, user=self.m_sServerDatabaseUser,
                                                         passwd=self.m_sServerDatabasePasswd,
                                                         database=self.m_sServerDatabase,
                                                         port=self.m_sServerPort, buffered=self.m_bServerSQLBuffered)
        aCursorDB_cryptomarkets = aMariaDB_cryptomarkets.cursor(dictionary=True)
        scrapedCutoffTime = (datetime.datetime.utcnow() -
                             datetime.timedelta(days=pdFrequencyDays)).strftime("%Y%m%d%H%M%S")
        query = "SELECT product_global_ID, product_market_ID FROM product_list WHERE cryptomarket_global_ID = {} AND " \
                "last_scraping_time_pd < {} LIMIT {};".format(self.m_nMarketGlobalID, scrapedCutoffTime, nBatchSize)
        aCursorDB_cryptomarkets.execute(query)
        productIdsToScrape = list(map(lambda x: [x['product_global_ID'], x['product_market_ID']],
                                      aCursorDB_cryptomarkets.fetchall()))
        aCursorDB_cryptomarkets.close()
        aMariaDB_cryptomarkets.close()
        return productIdsToScrape

    # Upload product descriptions to covid19, update product_list, product_desc_scraping_event in one transaction.
    def UpdateBatchOfProductDescriptions(self, lProductsDescInfo):
        pd_file_names = ' '.join(list(map(lambda x: x['product_desc_file_path_in_FS'], lProductsDescInfo)))
        sSCP_Command = "sshpass -p '{}' scp {} {}@{}:{}".format(self.m_sServerPasswd, pd_file_names,
                                                                self.m_sServerUser, self.m_sServerHost,
                                                                self.m_sRemoteRootDirectoryProductDesc)
        os.system(sSCP_Command)
        aMariaDB_cryptomarkets = mysql.connector.connect(host=self.m_sServerHost, user=self.m_sServerDatabaseUser,
                                                         passwd=self.m_sServerDatabasePasswd,
                                                         database=self.m_sServerDatabase, port=self.m_sServerPort,
                                                         buffered=self.m_bServerSQLBuffered)
        aMariaDB_cryptomarkets.autocommit = False
        aCursorDB_cryptomarkets = aMariaDB_cryptomarkets.cursor()
        try:
            updateProductListQuery = "UPDATE product_list SET last_scraping_time_pd = %s WHERE product_market_ID=%s " \
                                     "AND cryptomarket_global_ID = {}".format(self.m_nMarketGlobalID)
            updateProductListValues = list(map(lambda x: (x['scraping_time_pd'], x['product_market_ID']),
                                               lProductsDescInfo))
            aCursorDB_cryptomarkets.executemany(updateProductListQuery, updateProductListValues)
            insertPDScrapingEventQuery = "INSERT INTO product_desc_scraping_event (product_global_ID, scraping_time, " \
                                         "product_desc_file_path_in_FS, student_ID) VALUES (%s, %s, %s, {}) " \
                                         "ON DUPLICATE KEY UPDATE scraping_time = VALUES(scraping_time), " \
                                         "student_ID = {}, product_desc_file_path_in_FS = " \
                                         "VALUES(product_desc_file_path_in_FS);".format(self.m_sStudentIDinDB,
                                                                                        self.m_sStudentIDinDB)
            insertPDScrapingEventValues = list(map(lambda x: (x['product_global_ID'], x['scraping_time_pd'],
                                                              x['product_desc_file_path_in_FS'].split('/')[-1]),
                                                   lProductsDescInfo))
            aCursorDB_cryptomarkets.executemany(insertPDScrapingEventQuery, insertPDScrapingEventValues)
            aMariaDB_cryptomarkets.commit()
        except mysql.connector.Error as er:
            print('ERROR: \n', str(er))
            aMariaDB_cryptomarkets.rollback()
        finally:
            if aMariaDB_cryptomarkets.is_connected():
                aCursorDB_cryptomarkets.close()
                aMariaDB_cryptomarkets.close()

    # Given cutoff days and batch size, return vendor ids to scrape.
    def GetBatchOfVendorsToScrape(self, vpFrequencyDays, nBatchSize):
        aMariaDB_cryptomarkets = mysql.connector.connect(host=self.m_sServerHost, user=self.m_sServerDatabaseUser,
                                                         passwd=self.m_sServerDatabasePasswd,
                                                         database=self.m_sServerDatabase,
                                                         port=self.m_sServerPort, buffered=self.m_bServerSQLBuffered)
        aCursorDB_cryptomarkets = aMariaDB_cryptomarkets.cursor(dictionary=True)
        scrapedCutoffTime = (datetime.datetime.utcnow() -
                             datetime.timedelta(days=vpFrequencyDays)).strftime("%Y%m%d%H%M%S")
        query = "SELECT vendor_global_ID, vendor_market_ID FROM vendor_list WHERE cryptomarket_global_ID = {} AND " \
                "last_scraping_time_vp < {} LIMIT {};".format(self.m_nMarketGlobalID, scrapedCutoffTime, nBatchSize)
        aCursorDB_cryptomarkets.execute(query)
        vendorIdsToScrape = list(map(lambda x: [x['vendor_global_ID'], x['vendor_market_ID']],
                                     aCursorDB_cryptomarkets.fetchall()))
        aCursorDB_cryptomarkets.close()
        aMariaDB_cryptomarkets.close()
        return vendorIdsToScrape

    # Upload vendor profiles to covid19, update vendor_list, vendor_profile_scraping_event in one transaction.
    def UpdateBatchOfVendorProfiles(self, lVendorsProfileInfo):
        vp_file_names = ' '.join(list(map(lambda x: x['vendor_profile_file_path_in_FS'], lVendorsProfileInfo)))
        sSCP_Command = "sshpass -p '{}' scp {} {}@{}:{}".format(self.m_sServerPasswd, vp_file_names,
                                                                self.m_sServerUser, self.m_sServerHost,
                                                                self.m_sRemoteRootDirectoryVendorProfile)
        os.system(sSCP_Command)
        aMariaDB_cryptomarkets = mysql.connector.connect(host=self.m_sServerHost, user=self.m_sServerDatabaseUser,
                                                         passwd=self.m_sServerDatabasePasswd,
                                                         database=self.m_sServerDatabase, port=self.m_sServerPort,
                                                         buffered=self.m_bServerSQLBuffered)
        aMariaDB_cryptomarkets.autocommit = False
        aCursorDB_cryptomarkets = aMariaDB_cryptomarkets.cursor()
        try:
            updateVendorListQuery = "UPDATE vendor_list SET last_scraping_time_vp = %s WHERE vendor_market_ID = %s " \
                                    "AND cryptomarket_global_ID = {}".format(self.m_nMarketGlobalID)
            updateVendorListValues = list(map(lambda x: (x['scraping_time_vp'], x['vendor_market_ID']),
                                              lVendorsProfileInfo))
            aCursorDB_cryptomarkets.executemany(updateVendorListQuery, updateVendorListValues)
            insertVPScrapingEventQuery = "INSERT INTO vendor_profile_scraping_event (vendor_global_ID, scraping_time," \
                                         "vendor_profile_file_path_in_FS, student_ID) VALUES (%s, %s, %s, {}) " \
                                         "ON DUPLICATE KEY UPDATE scraping_time = VALUES(scraping_time), " \
                                         "student_ID = {}, vendor_profile_file_path_in_FS = " \
                                         "VALUES(vendor_profile_file_path_in_FS);".format(self.m_sStudentIDinDB,
                                                                                          self.m_sStudentIDinDB)
            insertVPScrapingEventValues = list(map(lambda x: (x['vendor_global_ID'], x['scraping_time_vp'],
                                                              x['vendor_profile_file_path_in_FS'].split('/')[-1]),
                                                   lVendorsProfileInfo))
            aCursorDB_cryptomarkets.executemany(insertVPScrapingEventQuery, insertVPScrapingEventValues)
            aMariaDB_cryptomarkets.commit()
        except mysql.connector.Error as er:
            print('ERROR: \n', str(er))
            aMariaDB_cryptomarkets.rollback()
        finally:
            if aMariaDB_cryptomarkets.is_connected():
                aCursorDB_cryptomarkets.close()
                aMariaDB_cryptomarkets.close()
