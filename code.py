"""
This module contains methods to test reporting search
"""
import json
import logging
import os
from datetime import timedelta, datetime
from enum import Enum
from http import HTTPStatus
from typing import Dict
import pandas as pd
import numpy as np

from assertpy import assert_that
from sqlalchemy import text

from base.base import Base
from products.pmp.controller import response_body_updater
from products.pmp.controller.request_response_json_generator import create_expected_response, \
    JSONTypes, get_test_data_from_suite_json, DEFAULT_JSON_PATH
from products.pmp.controller.response_body_updater import update_items_keys_for_product_listing
from utils import json_utils
from base import custom_logging
from products.pmp.controller.pmp_environment_variables import BASE_URI, PUBTOKEN_GLOBAL, \
    ENV_INTERNAL_ENDPOINT
from utils.database_utils import adflex_db_connection, komli_db_connection

logger = custom_logging.get_logger(__name__)
json_utils = json_utils.JsonUtils()


class ColumnsToSort(Enum):
    """
    Available sorting options for product listing api
    """
    modificationTime = "modificationTime"
    name = "name"


class ValidationLevels(Enum):
    """
    Validation Types
    """
    FULL_VALIDATION = 1
    COLUMN_WISE_SORTING_VALIDATION = 2
    METADATA_VALIDATION = 3


class DealTypes(Enum):
    """
    Deal Types
    """
    PMP = "PMP"
    AuctionPackage = "Auction Packages"
    PG = "Programmatic Guaranteed"
    PMPPreferred = "PMP Preferred"


# class ValidationLevels(Enum):
#     """
#     Validation Types
#     """
#     FULL_VALIDATION = 1
#     COLUMN_WISE_SORTING_VALIDATION = 2
#     METADATA_VALIDATION = 3


class SearchType(Enum):
    """
    SearchType
    """
    PRODUCT_NAME = 1
    DESCRITPION = 1


class ProductListingSearch(Base):
    """
    This class contains methods to test product listing search
    """

    def validate_product_listing_response(
            self,
            owner_details: Dict,
            **kwargs,
    ) -> None:
        """
        This method validates product listing search response
        Args:   owner_details

        :Keyword Arguments:
        * **Additional Arguments supported with kwargs**
        * *page_num* (``int``) -- page number of deal listing, default is 1
        * *page_size* (``int``) -- desired deal count in search result, default is 50
        * *deal_category* (``str``) -- Deal category - PMP/Auction Package/All
        * *search_string* (``str``) -- Deal Name/ID(s) in complete or partial mode
        * *search_type* (``SearchType``) -- Contains/Exact
        * *deal_status* (``List``) -- Allowed deal status in response listing
        * *marketplace* (``str``) -- All | PubMatic | GroupM
        * *always_on* (``int``) -- 0 denotes no, 1 denotes yes
        * *sort_by* (``str``) -- sorting column, must use ColumnsToSort Enum to select value
        * *is_descending* (``bool``) -- Boolean value to indicate sorting order
        * *from_date* (``str``) -- custom from date like {today_m_10_day}
        * *to_date* (``str``) -- custom start date like {today_p_10_day}
        * *request_type* (``str``) -- Request type, default is weekly
        * *status_code* (``int``) -- Expected status code of APi response
        * *buyer_type* (``int``) -- BUYER | PB | DP  (Mention type og user with a/c type 7)
        * *child_buyers* (``List``) -- To get deals for specific child of PB users
        * *is_child_buyer_deal_opted* (``bool``) -- If PB opted for child deal visibility
        * *validation_level* (``ValidationLevels``) -- To decide if full/only metadata/only sorting
        validation is required
        """

        # TODO 1. get the response from the actual product listing api
        # 2. get the expected response from the database
        # 3. compare the actual and expected response

        # get the response from the actual product listing api
        actual_response, filters = self.get_response_from_product_listing_api(owner_details,
                                                                              **kwargs)
        status_code = kwargs.get("status_code", HTTPStatus.OK.value)
        assert_that(actual_response.status_code).is_equal_to(status_code)

        # get expected response using db - use filters in this case
        expected_response = self.get_expected_result_of_reporting_search(owner_details, actual_response, **filters)
        # compare the actual and expected response

        #
        # json_utils.compare_expected_actual_json(
        #     expected_response, actual_response, delete_key_list=ignore_keys_product
        # )
        actual_response = actual_response.json
        keys_to_be_ignored = ['metrics', 'tz', 'dimensions']

        # if kwargs.get("validation_level") == ValidationLevels.COLUMN_WISE_SORTING_VALIDATION:
        #     keys_to_be_ignored.append("totalRecords")

        keys_to_be_verified = set(expected_response["metaData"].keys())
        all_keys_available = set(actual_response["metaData"].keys())
        keys_to_be_ignored.extend(list(all_keys_available - keys_to_be_verified))
        logger.info(f"Starting metadata validation, Ignore List: {keys_to_be_ignored}")
        json_utils.compare_expected_actual_json(
            expected_response["metaData"], actual_response["metaData"],
            delete_key_list=keys_to_be_ignored
        )
        if kwargs.get("is_negative_tc"):
            return None

        if filters["validation_level"] == ValidationLevels.COLUMN_WISE_SORTING_VALIDATION:
            actual_product_list = []
            for item in actual_response["items"]:
                actual_product_list.append(item['name'])

            # compare the product list
            logger.info(f"Constructed product list using actual response  {actual_product_list}")
            logger.info(f"Starting comparision.....")
            logger.info(f"Actual product list {actual_product_list} ")
            logger.info(f"Expected product list {expected_response['items']} ")
            assert_that(actual_product_list).is_equal_to(expected_response["items"])
        else:
            keys_to_be_ignored = []
            if len(expected_response["items"]):
                keys_to_be_verified = set(expected_response["items"][0].keys())
                all_keys_available = set(actual_response["items"][0].keys())
                keys_to_be_ignored.extend(list(all_keys_available - keys_to_be_verified))
            logger.info(f"Starting product items validation, Ignore List: {keys_to_be_ignored}")
            for i in range(len(expected_response["items"])):
                json_utils.compare_expected_actual_json(
                    expected_response["items"][i], actual_response["items"][i],
                    delete_key_list=keys_to_be_ignored
                )
        logger.info("Product Listing response validation successful")

    def get_response_from_product_listing_api(self, owner_details, **kwargs):
        """
        Returns Product Listing GET call response
        Args:
            owner_details ():
            **kwargs (): Similar like validate_reporting_search_response method

        Returns:

        """
        search_filter, filters, _ = self.convert_given_testdata_to_applicable_filters(owner_details,
                                                                                      **kwargs)
        pub_token = filters["pub_token"]
        response = self.send_request(
            method=self.RequestMethod.GET,
            headers={"PubToken": pub_token, "Content-Type": "application/json"},
            custom_url=f"{BASE_URI}/api/inventory/products/{search_filter}",

        )
        if response.status_code != HTTPStatus.OK.value:
            logger.info(f"Response: {response.json}")

        logger.info(f"Product Listing actual response {response}")
        logger.info(f"Filters dictionary : {filters}")
        return response, filters

    def post_response_from_reporting_search_api(self, owner_details, **kwargs):
        """
        Returns Reporting Search POST call response, Demand Side
        Args:
            owner_details ():
            **kwargs (): Similar like validate_reporting_search_response method

        Returns:

        """
        _, filters, deal_payload = self.convert_given_testdata_to_applicable_filters(owner_details,
                                                                                     **kwargs)
        pub_token = filters["pub_token"]
        response = self.send_request(
            method=self.RequestMethod.POST,
            payload=deal_payload,
            headers={"PubToken": pub_token, "Content-Type": "application/json"},
            custom_url=f"{BASE_URI}/api/pmp/deals/reportingSearch",
        )
        if response.status_code != HTTPStatus.OK.value:
            logger.info(f"Response: {response.json}")
        return response, filters

    def get_expected_result_of_reporting_search(self, owner_details, actual_response, **kwargs):
        """
        Generates expected response for reporting search api
        Args:
            owner_details ():
            **kwargs (): Similar like validate_reporting_search_response method

        Returns:

        """
        expected_json = {"metaData": {}, "items": []}
        if kwargs.get("validation_level") == ValidationLevels.COLUMN_WISE_SORTING_VALIDATION:

            sql_query = self.create_sql_query_for_product_listing(owner_details, **kwargs)

            rule_meta_df = adflex_db_connection.db_connection.execute(sql_query).fetchall()

            for item in rule_meta_df:
                # print(item)
                expected_json["items"].append(item[1])

            logger.info(f"Constructed product list using DB from  {expected_json['items']}")
            sort_string = kwargs.get("sort")

            json_path = os.path.join(DEFAULT_JSON_PATH, JSONTypes.PRODUCT_LISTING_RESPONSE.value)
            with open(json_path, encoding="utf-8") as file:
                product_response_json = json.load(file)

            expected_meta_data = self.update_expected_metadata(product_response_json, sort_string, owner_details,
                                                               **kwargs)
            expected_json["metaData"] = expected_meta_data
            return expected_json

        else:
            # what i need is ,
            #  read the data from test data file
            #  manipulate default json
            # and return the expected response
            # sql_query = self.create_sql_query_for_product_listing(owner_details, **kwargs)

            # rule_meta_df = pd.DataFrame(
            #     adflex_db_connection.db_connection.execute(sql_query).fetchall())

            # data_to_be_updated = []
            #
            # expected_response = create_expected_response(
            #     JSONTypes.PRODUCT_LISTING_RESPONSE,
            #     owner_details,
            #     data_to_be_updated=data_to_be_updated,
            #     is_negative_tc=False
            # )

            # logger.info(f"product listing expected_response: {expected_response}")
            sort_string = kwargs.get("sort")

            json_path = os.path.join(DEFAULT_JSON_PATH, JSONTypes.PRODUCT_LISTING_RESPONSE.value)
            with open(json_path, encoding="utf-8") as file:
                product_response_json = json.load(file)

            expected_json = {"metaData": {}, "items": []}
            expected_meta_data = self.update_expected_metadata(product_response_json, sort_string, owner_details,
                                                               **kwargs)
            expected_json["metaData"] = expected_meta_data

            if kwargs.get("is_negative_tc"):
                return expected_json

            items = product_response_json["items"]

            for item in items:
                if actual_response.json['items'][0]["name"] is not None:
                    item["name"] = actual_response.json['items'][0]["name"]

                if actual_response.json['items'][0]["description"] is not None:
                    item["description"] = actual_response.json['items'][0]["description"]

                if actual_response.json['items'][0]["tags"] is not None:
                    item["tags"] = actual_response.json['items'][0]["tags"]

                if actual_response.json['items'][0]["id"] is not None:
                    item["id"] = actual_response.json['items'][0]["id"]

            test_data_json = get_test_data_from_suite_json()

            items_updated = update_items_keys_for_product_listing(test_data_json, items[0], **kwargs)

            expected_json["items"].append(items_updated)

            print("Final Expected JSON: ", expected_json)
            return expected_json

    def update_expected_metadata(self, expected_json, sort_string, owner_details, **kwargs):

        metDatajson = expected_json["metaData"]
        transaction_count_filter = ""
        # default values
        metDatajson["request"]["pageSize"] = kwargs.get("page_size")

        metDatajson["request"]["pageNumber"] = kwargs.get("page_num")
        # metDatajson["request"]["pageNumber"] = 0

        metDatajson["request"]["fromDate"] = "2010-01-02"
        metDatajson["request"]["toDate"] = "2099-01-01"

        # sort string manipulation
        if kwargs.get("sort") is False:
            metDatajson["request"]["sort"] = "null"
        else:
            if kwargs.get("is_descending"):
                metDatajson["request"]["sort"] = f"-{sort_string.value}"
            else:
                metDatajson["request"]["sort"] = sort_string.value

        if kwargs.get("validation_level") == ValidationLevels.COLUMN_WISE_SORTING_VALIDATION:
            if kwargs.get("search_criteria") == "transactionCount":
                count = kwargs.get("transactionCount")
                transaction_count_filter = 'transactionCount eq count'.replace("count", str(count))
                metDatajson["request"]["filters"] = [transaction_count_filter]
            else:
                metDatajson["request"]["filters"] = []

            # update start index based on page_num and page_size
            metDatajson["startIndex"] = kwargs.get("page_size") * (kwargs.get("page_num") - 1) + 1

            metDatajson["endIndex"] = kwargs.get("page_size") * kwargs.get("page_num")

            totalrecords = self.get_total_records_from_db(owner_details, **kwargs)
            logger.info(f"total records fetched from DB  {totalrecords}")
            metDatajson["totalRecords"] = totalrecords

            # TODO remove the hardcoded pagenumber value of zero after bug is fixed - https://inside.pubmatic.com:9443/jira/browse/PMPA-14102
            # metDatajson['request']['originalQuery'] = f"pageSize={kwargs.get('page_size')}&pageNumber={kwargs.get('page_num')}&fromDate=2010-01-02&toDate=2099-01-01"
            metDatajson['request'][
                'originalQuery'] = f"pageSize={kwargs.get('page_size')}&pageNumber={(kwargs.get('page_num') - 1)}&fromDate=2010-01-02&toDate=2099-01-01"
            if kwargs.get("search_criteria") == "transactionCount":
                metDatajson['request'][
                    'originalQuery'] += f"&filters={transaction_count_filter}"

        else:
            # metDatajson = expected_json["metaData"]
            metDatajson["startIndex"] = 1
            # this needs to be updated using db, since there is edge case where description and product name is same
            # TODO : update this using db and write the function to fetch total records using sql
            metDatajson["totalRecords"] = 1
            # this needs to be updated using db, since there is edge case where description and product name is same
            metDatajson["endIndex"] = 1

            if kwargs.get("search_criteria") == "product_id":
                criteria = kwargs.get("product_id")
                metDatajson["request"]["filters"] = [
                    "id eq criteria".replace("criteria", str(criteria))]
                metDatajson["request"]["originalQuery"] = (
                    # f"pageSize={kwargs.get('page_size')}&pageNumber={kwargs.get('page_num')}&fromDate=2010-01-02&toDate=2099-01-01"
                    f"pageSize={kwargs.get('page_size')}&pageNumber={(kwargs.get('page_num') - 1)}&fromDate=2010-01-02&toDate=2099-01-01"
                    "&filters=id eq criteria").replace(
                    "criteria", str(criteria))

            elif kwargs.get("search_criteria") == "product_name":
                criteria = kwargs.get("product_name")
                metDatajson["request"]["filters"] = [
                    "name like *criteria*".replace("criteria", criteria)]
                metDatajson["request"]["originalQuery"] = (
                    # f"pageSize={kwargs.get('page_size')}&pageNumber={kwargs.get('page_num')}&fromDate=2010-01-02&toDate=2099-01-01"
                    f"pageSize={kwargs.get('page_size')}&pageNumber={(kwargs.get('page_num') - 1)}&fromDate=2010-01-02&toDate=2099-01-01"
                    "&filters=name like *criteria*").replace(
                    "criteria", criteria)
            elif kwargs.get("search_criteria") == "description":
                criteria = kwargs.get("description")
                metDatajson["request"]["filters"] = [
                    "description like *criteria*".replace("criteria", criteria)]
                metDatajson["request"]["originalQuery"] = (
                    # f"pageSize={kwargs.get('page_size')}&pageNumber={kwargs.get('page_num')}&fromDate=2010-01-02&toDate=2099-01-01"
                    f"pageSize={kwargs.get('page_size')}&pageNumber={(kwargs.get('page_num') - 1)}&fromDate=2010-01-02&toDate=2099-01-01"
                    "&filters=description like *criteria*").replace(
                    "criteria", criteria)
            elif kwargs.get("search_criteria") == "tags":
                criteria = kwargs.get("tags")
                metDatajson["request"]["filters"] = [
                    "tags like *criteria*".replace("criteria", criteria)]
                metDatajson["request"]["originalQuery"] = (
                    # f"pageSize={kwargs.get('page_size')}&pageNumber={kwargs.get('page_num')}&fromDate=2010-01-02&toDate=2099-01-01"
                    f"pageSize={kwargs.get('page_size')}&pageNumber={(kwargs.get('page_num') - 1)}&fromDate=2010-01-02&toDate=2099-01-01"
                    "&filters=tags like *criteria*").replace(
                    "criteria", criteria)
            elif kwargs.get("search_criteria") == "both":
                criteria_desc = kwargs.get("description")
                criteria_prod = kwargs.get("product_name")
                metDatajson["request"]["filters"] = [
                    "name like *criteria_prod*,description like *criteria_desc*".replace("criteria_prod",
                                                                                         criteria_prod).replace(
                        "criteria_desc", criteria_desc)]
                metDatajson["request"]["originalQuery"] = (
                    # f"pageSize={kwargs.get('page_size')}&pageNumber={kwargs.get('page_num')}&fromDate=2010-01-02&toDate=2099-01-01"
                    f"pageSize={kwargs.get('page_size')}&pageNumber={(kwargs.get('page_num') - 1)}&fromDate=2010-01-02&toDate=2099-01-01"
                    "&filters=name like *criteria_prod*,description like *criteria_desc*").replace("criteria_prod",
                                                                                                   criteria_prod).replace(
                    "criteria_desc", criteria_desc)
            else:
                criteria = kwargs.get("product_name")  # default case
                metDatajson["request"]["filters"] = [
                    "name like *criteria*,description like *criteria*,tags like *criteria*".replace("criteria",
                                                                                                    criteria)]
                # metDatajson["request"]["originalQuery"] = (f"pageSize={kwargs.get('page_size')}&pageNumber={kwargs.get('page_num')}&fromDate=2010-01-02&toDate=2099-01"
                metDatajson["request"]["originalQuery"] = (
                    f"pageSize={kwargs.get('page_size')}&pageNumber={(kwargs.get('page_num') - 1)}&fromDate=2010-01-02&toDate=2099-01"
                    "-01&filters=name like *criteria*,")
                "description like *criteria*,tags like *criteria*".replace(
                    "criteria", criteria)

            if kwargs.get("is_negative_tc"):
                metDatajson["totalRecords"] = 0
                metDatajson["endIndex"] = 0

        logger.info(f"constructed expected metadata json  {metDatajson}")
        return metDatajson

    def get_total_records_from_db(self, owner_details, **kwargs):
        """
        To fetch product count of specific filter conditions
        Args:
            owner_details ():
            **kwargs ():

        Returns:


        """
        transaction_count_filter = ""
        archived_filter = " AND archived = 0"
        if kwargs.get("archive"):
            archived_filter = " AND archived = 1"
        if kwargs.get("transactionCount"):
            transaction_count_filter = f" AND p.transaction_count = {kwargs.get('transactionCount')}"
        sql_query = (
            f"SELECT DISTINCT count(*) "
            f" FROM KomliAdServer.product p "
            f" WHERE p.deleted = 0 "
            f" AND p.internal = false "
            f" AND p.owner_type = {owner_details['owner_type']} "
            f" AND p.owner_id = {owner_details['owner_id']} "
            f" {transaction_count_filter} "
            f" {archived_filter} ;"
        )
        logger.info(f"count products SQL query: {sql_query}")
        result = komli_db_connection.db_connection.execute(sql_query).fetchall()
        logger.info(f"Current Product Count: {result[0][0]}")
        return result[0][0]

    @staticmethod
    def convert_given_testdata_to_applicable_filters(owner_details, **kwargs):
        """
        This Method generated api URl filters for GET call and payload for POST call or
        reporting search
        Args:
            owner_details ():
            **kwargs (): Similar like validate_reporting_search_response method

        Returns:

        """
        # ?filters = loggedInOwnerId + eq + 33233
        # & filters = loggedInOwnerTypeId + eq + 1
        # & pageNumber = 1
        # & pageSize = 10
        # & sort = -modificationTime

        filters = {"page_num": 1, "page_size": 10, "status_code": HTTPStatus.OK.value,
                   "sort": ColumnsToSort.modificationTime, "is_descending": True, "pub_token": PUBTOKEN_GLOBAL,
                   'validation_level': ValidationLevels.FULL_VALIDATION}

        # if filters["validation_level"] == ValidationLevels.COLUMN_WISE_SORTING_VALIDATION:
        filters.update(kwargs)

        post_call_payload = {

            "request": {
                "pageSize": filters["page_size"],
                "pageNumber": filters["page_num"],
                "sort": '-' + str(filters["sort"].name) if filters["is_descending"]
                else filters["sort"].name
            }
        }
        search_filter = ""

        post_call_filters = []

        search_filter += (
            f"?pageNumber={filters['page_num']}"
        )
        search_filter += f"&pageSize={filters['page_size']}"
        search_filter += f"&filters=loggedInOwnerId%20eq%20{owner_details['owner_id']}"
        search_filter += (
            f"&filters=loggedInOwnerTypeId%20eq%20{owner_details['owner_type']}"
        )
        search_filter += (
            f"&sort={post_call_payload['request']['sort']}"
        )

        if kwargs.get("search_criteria") == "product_name":
            search_filter += f"&filters=name%20like%20*{kwargs.get('product_name')}*"

        if kwargs.get("search_criteria") == "description":
            search_filter += f"&filters=description%20like%20*{kwargs.get('description')}*"

        if kwargs.get("search_criteria") == "both":
            if kwargs.get("product_name") is None:
                search_using = kwargs.get("description")
            else:
                search_using = kwargs.get("product_name")
            search_filter += f"&filters=description%20like%20*{search_using}*"
            search_filter += f"&filters=name%20like%20*{search_using}*"

        if kwargs.get("search_criteria") == "tags":
            search_filter += f"&filters=tags%20like%20*{kwargs.get('tags')}*"

        if kwargs.get("archive"):
            search_filter += f"&archive=1"

        # Optional Filters
        if kwargs.get("search_criteria") == "product_id":
            search_filter += f"&filters=id+eq+{kwargs.get('product_id')}"

        if kwargs.get("search_criteria") == "transactionCount":
            search_filter += f"&filters=transactionCount+eq+{kwargs.get('transactionCount')}"

        post_call_filters.append(f"loggedInOwnerId eq {owner_details['owner_id']}")
        post_call_filters.append(f"loggedInOwnerTypeId eq {owner_details['owner_type']}")

        # for the search criteria on create deals page
        if kwargs.get("derivedinventoryrequest"):
            search_filter += f"&derivedInventoryRequest=true"

        post_call_payload["request"]["filters"] = post_call_filters
        logger.info(f"Converted Filters to be applied: {search_filter}")
        return search_filter, filters, post_call_payload

    from sqlalchemy import text

    # @staticmethod
    # def create_sql_query_for_product_listing(owner_details, **kwargs):
    #     """
    #     This method generates SQL query with given applicable filters to find out eligible deals
    #     Args:
    #         owner_details (dict): A dictionary containing owner details.
    #         search_string (str): The search string for filtering names and descriptions.
    #         **kwargs: Additional filters and options.
    #
    #     Returns:
    #         str: The generated SQL query.
    #     """
    #
    #     # Default settings
    #
    #     # Default page Number and page size
    #     pageNumber = 1
    #     pageSize = 10
    #
    #     # Default sort filter
    #     sort_filter = "modification_time DESC"
    #
    #     # By Default all active products will be fetched
    #     archived_filter = " AND archived = :archived"
    #
    #     # By Default internal will be false since we are fetching external products
    #     internal_filter = " AND internal = :internal"
    #
    #     # By Default search filter condition is empty since it is a default case
    #     search_filter_condition = ""
    #
    #     adflex_db_connection.db_connection.execute("set time_zone = '+00:00';")
    #
    #     # Construct search filter for default case, In this case no search_filter is needed
    #     params = {
    #         "product_name": None,
    #         "product_description": None,
    #         "internal": False,
    #         "archived": 0,
    #         "owner_type": owner_details['owner_type'],
    #         "owner_id": owner_details['owner_id'],
    #         "limit": pageSize,
    #         "offset": (pageNumber - 1) * pageSize
    #     }
    #     # if kwargs.get("search_criteria") == "product_id":
    #     #     search_filter += f"&filters=id+eq+{kwargs.get('product_id')}"
    #     #
    #     # if kwargs.get("search_criteria") == "transactionCount":
    #     #     search_filter += f"&filters=id+eq+{kwargs.get('transactionCount')}"
    #     if kwargs.get("search_criteria") == "transactionCount":
    #         search_filter_condition = "AND p.transaction_count = :transactionCount"
    #         params["transactionCount"] = f"%{kwargs.get('transactionCount')}%"
    #
    #     elif kwargs.get("search_criteria") == "product_id":
    #         search_filter_condition = "AND p.id = :product_id"
    #         params["product_id"] = f"%{kwargs.get('product_id')}%"
    #
    #     elif kwargs.get("search_criteria") == "name":
    #         search_filter_condition = "AND p.name like :product_name"
    #         params["product_name"] = f"%{kwargs.get('product_name')}%"
    #
    #     elif kwargs.get("search_criteria") == "description":
    #         search_filter_condition = "AND p.description like :product_description"
    #         params["product_description"] = f"%{kwargs.get('description')}%"
    #
    #     elif kwargs.get("search_criteria") == "both":
    #         search_filter_condition = "AND (p.name like :product_name OR p.description like :product_description)"
    #         params["product_name"] = f"%{kwargs.get('product_name')}%"
    #         params["product_description"] = f"%{kwargs.get('description')}%"
    #
    #     # to get the internal products, in case of external products internal filter will be false
    #     if kwargs.get("internal") is True:
    #         params["internal"] = True
    #
    #     # to get the de-active products
    #     if kwargs.get("archive"):
    #         params["archived"] = 1
    #
    #     # to get the products based on owner type and owner id
    #     owner_type_filter = " AND p.owner_type = :owner_type"
    #     owner_id_filter = " AND p.owner_id = :owner_id"
    #
    #     # sort by modification time by default DESC
    #     if kwargs.get("sort").value == "name":
    #         sort_filter = "name"
    #         if kwargs.get("is_descending"):
    #             sort_filter += " DESC"
    #
    #     if kwargs.get("page_num"):
    #         pageNumber = kwargs.get("page_num")
    #         params["offset"] = (pageNumber - 1) * pageSize
    #
    #     if kwargs.get("page_size"):
    #         pageSize = kwargs.get("page_size")
    #         params["limit"] = pageSize
    #
    #     # Construct SQL query with parameter placeholders
    #     sql_query = text(f"""
    #     SELECT * FROM (
    #         SELECT DISTINCT p.id, p.name, p.description, p.creation_time, p.deleted, p.archived,
    #                         p.modification_time, p.owner_id, p.owner_type, p.transaction_count, p.used_count,
    #                         p.site_name, p.tags, p.internal, p.inline, p.companion_ads, p.min_ad_duration,
    #                         p.max_ad_duration, p.product_type, p.min_selected_viewability
    #         FROM KomliAdServer.product p
    #         WHERE p.deleted = 0 {search_filter_condition} {internal_filter} {archived_filter} {owner_type_filter} {owner_id_filter}
    #         UNION
    #         SELECT DISTINCT p.id, p.name, p.description, p.creation_time, p.deleted, p.archived,
    #                         p.modification_time, p.owner_id, p.owner_type, p.transaction_count, p.used_count,
    #                         p.site_name, p.tags, p.internal, p.inline, p.companion_ads, p.min_ad_duration,
    #                         p.max_ad_duration, p.product_type, p.min_selected_viewability
    #         FROM KomliAdServer.product p
    #         JOIN KomliAdServer.product_entity pe ON pe.product_id = p.id
    #         JOIN KomliAdServer.discoverable_mapping dm ON pe.entity_id = dm.id
    #         JOIN KomliAdServer.discoverable_to_partner dtp ON dm.id = dtp.discoverable_mapping_id
    #         WHERE p.deleted = 0 {search_filter_condition} {internal_filter} {archived_filter}
    #           AND pe.entity_type_id = 40
    #           AND dtp.resource_type_id = 1
    #           AND (dtp.resource_id = :owner_id OR dtp.resource_id = 0)
    #     ) AS p
    #     ORDER BY {sort_filter}
    #     LIMIT :limit OFFSET :offset
    #     """)
    #
    #     logger.info(f"SQL query: {sql_query}")
    #
    #     return sql_query, params

    @staticmethod
    def create_sql_query_for_product_listing(owner_details, **kwargs):
        """
        This method generates SQL query with given applicable filters to find out eligible deals
        Args:
            owner_details (dict): A dictionary containing owner details.
            search_string (str): The search string for filtering names and descriptions.
            **kwargs: Additional filters and options.

        Returns:
            str: The generated SQL query.
        """

        # Default settings

        # Default page Number and page size
        pageNumber = 1
        pageSize = 10

        if kwargs.get("derivedinventoryrequest"):
            pageSize = 20

        # Default sort filter
        sort_filter = "modification_time DESC"

        # By Default all active products will be fetched
        archived_filter = " AND archived = 0"

        # by default we wu

        # By Default internal will be false since we are fetching external products
        internal_filter = " AND internal = false"

        # By Default search filter condition is empty since it is a default case
        search_filter_condition = ""

        adflex_db_connection.db_connection.execute("set time_zone = '+00:00';")

        # Construct search filter for default case, In this case no search_filter is needed
        if kwargs.get("search_criteria") == "transactionCount":
            search_filter_condition = f"AND p.transaction_count = {kwargs.get('transactionCount')}"

        elif kwargs.get("search_criteria") == "product_id":
            search_filter_condition = f"AND p.id = {kwargs.get('product_id')}"

        elif kwargs.get("search_criteria") == "name":
            # search_filter = "name"
            search_filter_condition = f"AND p.name like '%{kwargs.get('product_name')}%'"

        elif kwargs.get("search_criteria") == "description":
            # search_filter = "name"
            # f"AND p.name like '%{kwargs.get('description')}%'"
            search_filter_condition = f"AND p.name like '%{kwargs.get('description')}%'"

        elif kwargs.get("search_criteria") == "both":
            search_filter_condition = f"AND (p.name like '%{kwargs.get('product_name')}%' or p.description like '%{kwargs.get('description')}%') "

        # to get the internal products, in case of external products internal filter will be false
        if kwargs.get("internal") is True:
            internal_filter = " AND internal = true"

        # to get the de-active products
        if kwargs.get("archive"):
            archived_filter = " AND archived = 1"

        # to get the products based on owner type and owner id
        owner_type_filter = f" AND p.owner_type = {owner_details['owner_type']}"
        owner_id_filter = f" AND p.owner_id = {owner_details['owner_id']}"

        # sort by modification time by default DESC
        if kwargs.get("sort").value == "name":
            sort_filter = " name"
            if kwargs.get("is_descending"):
                sort_filter = " name DESC"


        if kwargs.get("page_num"):
            pageNumber = kwargs.get("page_num")

        if kwargs.get("page_size"):
            pageSize = kwargs.get("page_size")

        # if kwargs.get("default") == True:
        #     limit_filter = ""
        # else:
        #     limit_filter = f"LIMIT 0, {pageSize * pageNumber}"
        # # else:
        # #     limit_filter = f"LIMIT {pageSize * (pageNumber - 1)}, {pageSize}"
        # Calculate the offset
        offset = (pageNumber - 1) * pageSize
        # Construct SQL query
        sql_query = (
            f"SELECT * FROM ("
            f" SELECT DISTINCT p.id, p.name, p.description, p.creation_time, p.deleted, p.archived, "
            f" p.modification_time, p.owner_id, p.owner_type, p.transaction_count, p.used_count, "
            f" p.site_name, p.tags, p.internal, p.inline, p.companion_ads, p.min_ad_duration, "
            f" p.max_ad_duration, p.product_type, p.min_selected_viewability "
            f" FROM KomliAdServer.product p "
            f" WHERE p.deleted = 0 {search_filter_condition} {internal_filter} {archived_filter} {owner_type_filter} {owner_id_filter} "
            f" UNION "
            f" SELECT DISTINCT p.id, p.name, p.description, p.creation_time, p.deleted, p.archived, "
            f" p.modification_time, p.owner_id, p.owner_type, p.transaction_count, p.used_count, "
            f" p.site_name, p.tags, p.internal, p.inline, p.companion_ads, p.min_ad_duration, "
            f" p.max_ad_duration, p.product_type, p.min_selected_viewability "
            f" FROM KomliAdServer.product p, KomliAdServer.product_entity pe, KomliAdServer.discoverable_mapping dm, "
            f" KomliAdServer.discoverable_to_partner dtp "
            f" WHERE p.deleted = 0 {search_filter_condition} {internal_filter} {archived_filter} "
            f" AND pe.product_id = p.id "
            f" AND pe.entity_type_id = 40 AND pe.entity_id = dm.id AND dm.id = dtp.discoverable_mapping_id "
            f" AND ( dtp.resource_type_id = 1 AND (dtp.resource_id = {owner_details['owner_id']} OR dtp.resource_id = 0) ) "
            f") AS p "
            f"ORDER BY {sort_filter} "
            f"LIMIT {pageSize} OFFSET {offset};"

        )

        logger.info(f"SQL query: {sql_query}")

        return sql_query
