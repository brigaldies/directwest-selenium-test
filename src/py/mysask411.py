import sys
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import win32com.client
import time

import unittest

import pandas as pd


# def usage():
#     print('Usage {0} {1}'.format(sys.argv[0], '<MySask411 site URL>'))


class TestBusinessNamesHyphenated(unittest.TestCase):
    def setUp(self):
        print('Tests setup')

        # Instantiate the Selenium WebDriver
        self.driver = webdriver.Chrome()

        # Connecting to the MySask411 site under test
        # TODO: Specify the environment on the command line.
        # DEV
        url = 'https://osc.mysask411.net'
        # PROD
        # url = 'https://mysask411.com'

        print('Connecting to {0}'.format(url))
        self.driver.get(url)

        if url == 'https://osc.mysask411.net':
            # MySask411 authentication on DEV
            print('Authenticating...')
            time.sleep(1)
            shell = win32com.client.Dispatch("WScript.Shell")
            shell.Sendkeys("mysask411")
            time.sleep(1)
            shell.Sendkeys("{TAB}")
            time.sleep(1)
            shell.Sendkeys("nj8yH4")
            time.sleep(1)
            shell.Sendkeys("{ENTER}")
            time.sleep(1)

            try:
                print('Waiting for the site...')
                searchbox_what = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.NAME, "what"))
                )
                self.assertIsNotNone(searchbox_what, "Can't find the 'what' input field!")

            except TimeoutException:
                print("TimeoutException: Waiting for site")
                self.assertTrue(False, "Timed out waiting for the site!")

        # Verify we're on the proper site
        self.assertTrue("Find Local Businesses" in self.driver.title)
        self.assertTrue("mysask411" in self.driver.page_source)

        testplan_file = 'C:\\Users\\oscadmin\\Downloads\\DirectWest QA Template.xlsx'
        print("Reading test data from {0}...".format(testplan_file))
        self.xl = pd.ExcelFile(testplan_file)
        self.assertIsNotNone(self.xl)
        print(self.xl.sheet_names)
        self.df = self.xl.parse('Bus Name Hyphens')
        print(list(self.df.columns.values))

    def test_business_names_hyphenated(self):
        # TODO: Relocate the loop into a common function so that it can be called from other test functions.
        self.tests_count = 0
        self.success_count = 0
        for index, row in self.df.iterrows():
            with self.subTest(i=index):
                self.tests_count += 1
                test_scenario = row['Test Scenario'].strip()
                business_name = row['Search String'].strip()
                business_location = row['Location'].strip()
                expected_business_id = row['Expected Top Result']
                print("{0}: {1}, {2}, {3}, expecting top result {4}".format(index + 1, test_scenario, business_name,
                                                                            business_location, expected_business_id))

                # "What" search box
                searchbox_what = self.driver.find_element_by_name("what")
                self.assertIsNot(searchbox_what, "Can't find the 'what' input field!")
                searchbox_what.clear()
                search_what = business_name
                searchbox_what.send_keys(search_what)

                # "Where" search box
                searchbox_where = self.driver.find_element_by_name("where")
                self.assertIsNotNone(searchbox_where, "Can't find the 'where' input field!")
                searchbox_where.clear()
                search_where = business_location
                searchbox_where.send_keys(search_where)

                # Send the search
                print('Search "{0}" in "{1}"'.format(search_what, search_where))
                search_button = self.driver.find_element_by_tag_name('button')
                self.assertIsNotNone(search_button, "Can't find the search button!")
                # search_button.click()

                # We can also launch
                searchbox_where.send_keys(Keys.RETURN)

                # Wait for the results
                try:
                    print('Waiting for results...')
                    element = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "listings-title"))
                    )
                    # TODO: The get_attribute below does not return the expected title <search> in <location>
                    # print('Listings title: {0}'.format(element.get_attribute('text')))

                    # Assert results
                    # assert search_what + ' in ' + search_where in driver.page_source
                except TimeoutException:
                    self.assertTrue(False, "Timed out waiting for the search results!")

                # Extract results
                print('Extract results...')
                listings = self.driver.find_elements_by_class_name('listing-title')
                self.assertIsNotNone(listings, "Can't find the 'listing-title' element!")

                # count = 0
                # for listing in listings:
                #     count = count + 1
                #     listing_anchor = listing.find_element_by_tag_name('a')
                #     assert listing_anchor
                #     listing_text = listing_anchor.get_attribute("text")
                #     listing_href = listing_anchor.get_attribute("href")
                #     print('Listing {0}: "{1}", href={2}'.format(count, listing_text, listing_href))

                # Extract top result
                top_listing = listings[0]
                listing_anchor = top_listing.find_element_by_tag_name('a')
                self.assertIsNotNone(listing_anchor, "Can't find the listing's anchor tag!")
                listing_text = listing_anchor.get_attribute("text")
                listing_href = listing_anchor.get_attribute("href")
                print('Top listing : "{0}", href={1}\n'.format(listing_text, listing_href))

                # Extracting the business listing ID at the end of the URL
                business_listing_id = int(listing_href.split('/')[-1])
                self.assertEqual(business_listing_id, expected_business_id,
                                 "Unexpected top result {0}".format(business_listing_id))
                self.success_count += 1

                # TODO: Re-run the test on PROD and compare the results!

    # TODO: Function to test the ampersand in business names

    # TODO: Function to test the apostrophe (possessive form) in business names

    def tearDown(self):
        # Pause to view the web site
        print(
            "Tests count = {0}, successes count = {1}, percent success={2}%".format(self.tests_count, self.success_count,
                                                                                   round((
                                                                                         self.success_count / self.tests_count) * 100)))
        input("Press Enter to continue...")
        print('Closing Selenium WebDriver...')
        self.driver.close()
        print('End of tests.')


if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     usage()
    #     exit(1)

    unittest.main()
