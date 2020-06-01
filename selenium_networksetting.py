import os
import logging
from selenium import webdriver
from selenium.webdriver.remote.remote_connection import LOGGER


def selenium_setup_firefox_network():
    # Setup Firefox browser for visiting .onion sites through Tor (AWS proxy)
    # sCommandSSH_AWS = "ssh leek -L 50040:127.0.0.1:9050 -f -N -p 60022"
    # os.system(sCommandSSH_AWS)
    LOGGER.setLevel(logging.ERROR)  # to remove geckodriver.log
    aProfile = webdriver.FirefoxProfile()  # open firefox, enter "about:config"
    # manual javascript disabled its needed for genesis market
    aProfile.DEFAULT_PREFERENCES['frozen']['javascript.enabled'] = False
    aProfile.set_preference("network.proxy.type", 1)  # Manual proxy configuration
    aProfile.set_preference("network.proxy.socks", "127.0.0.1")  # SOCKS Host # local proxy
    # You can use any port, make sure it is consistent with the port used in the above SSH command.
    aProfile.set_preference("network.proxy.socks_port", 9050)
    aProfile.set_preference("network.proxy.socks_remote_dns", True)  # Proxy DNS when using SOCKS v5
    aProfile.set_preference("network.dns.blockDotOnion", False)  # do not block .onion domains
    aProfile.set_preference("browser.download.folderList", 2) # for changing the download folder
    aProfile.set_preference("browser.download.manager.showWhenStarting", False)
    aProfile.set_preference("browser.download.dir", "/home/rob/temp_scrapedhtml/")

    aBrowserDriver = webdriver.Firefox(aProfile, executable_path='/home/rob/geckodriver-v0.25.0-linux64/geckodriver',
                                       log_path=os.devnull)
    aBrowserDriver.set_page_load_timeout(30000)

    return aBrowserDriver
