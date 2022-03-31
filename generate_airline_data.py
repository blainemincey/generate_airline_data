import time
import datetime
from timeit import default_timer as timer
import settings
from pymongo import MongoClient
from faker import Faker
from faker_airtravel import AirTravelProvider
import random

faker = Faker()
faker.add_provider(AirTravelProvider)

####
# Start script
####
startTs = time.localtime()
start = timer()
print("======================================")
print("       Generating Airline Data        ")
print("======================================")
print("\nStarting " + time.strftime("%Y-%m-%d %H:%M:%S", startTs) + "\n")


####
# Main start function
####
def main():
    print('Begin generating airline documents.')
    print('Number of documents to generate: ' + str(NUM_DOCS))

    # init db
    mdb_connection = get_database_connection()
    init(mdb_connection)
    flight_reservation_collection = mdb_connection[MDB_COLLECTION]

    # array of generated documents.  Bulk insert every 1000.
    flight_reservation_data = []
    document_count = 0

    for index in range(int(NUM_DOCS)):
        # get unique document
        flight_reservation_document = get_fullfillment_info()

        # add to set for bulk insert
        flight_reservation_data.append(flight_reservation_document)

        document_count += 1

        # Indicate how many docs inserted
        if len(flight_reservation_data) % 1000 == 0:
            flight_reservation_collection.insert_many(flight_reservation_data)
            print('Docs inserted: ' + str(document_count))
            flight_reservation_data.clear()


####
# Grab the database connection
####
def get_database_connection():
    print('Get database connection.')
    mongo_client = MongoClient(MDB_CONNECTION)
    database = mongo_client[MDB_DATABASE]

    return database


##
#  Initialize db.  Drop existing collection to start fresh.
##
def init(database):
    print('Initialize database.')
    flight_reservation_collection = database[MDB_COLLECTION]

    print('Drop Flight Reservation Collection.')
    flight_reservation_collection.drop()


####
# Generate a fullfillment_info document
####
def get_fullfillment_info():
    city_code = random.choice(['ATL', 'BOS', 'DET', 'NYC', 'SEA'])

    # call other methods to populate
    origin_code = faker.airport_iata()
    dest_code = faker.airport_iata()

    # origin flight date 2 to 30 days ago
    origin_flt_date = faker.date_time_between(start_date='-30d', end_date='-2d', tzinfo=None)
    # return flight up to 5 days ahead
    return_flt_date = faker.future_datetime(end_date='+5d')

    # Grab the add'l arrays required to build fullfillment_info document
    refund_data_list = get_refund_data_list(origin_code, origin_flt_date, dest_code, return_flt_date)
    pnr_segment = get_pnr_segment(origin_code, origin_flt_date, dest_code, return_flt_date)

    fullfillment_document = {
        "fulfillmentInfo": {
            "intentCriteria": {
                "callerIntentText": "REFUND"
            },
            "refundDataList": refund_data_list,
            "refundTransactionId": faker.uuid4(),
            "travelContextText": "Vol",
            "travelContextDesc": "Vol",
            "pnrSegment": pnr_segment,
            "eligibilitystatus": {
                "hasRemarks": False
            }
        },
        "sender": {
            "reservationAgent": {
                "agentId": random.randint(1, 50),
                "agentRoleId": "RESGENSALES",
                "cityCode": city_code
            },
            "senderCode": "OMP",
            "testLabName": "TSBB",
            "inactiveSessionTimeoutSecondCnt": 600,
            "sessionTimeoutSecondCnt": 601,
            "pointOfSale": {
                "countryCode": "US",
                "pointOfSaleCityCode": city_code,
                "soldByTravelAgency": True,
                "pointOfSaleId": city_code + "RES",
                "vdnCode": random.randint(1, 100),
                "customerId": random.randint(1, 10000)
            }
        }
    }

    return fullfillment_document


####
# Generate the refund data list
####
def get_refund_data_list(origin_code, origin_flt_date, dest_code, return_flt_date):
    document_number = faker.uuid4()
    # tickets reserved up to year ago
    issue_local_date = faker.date_time_between(start_date='-1y', end_date='now', tzinfo=None)
    passenger_type_code = random.choice(['ADT', 'CHD', 'INF', 'INS', 'UNN'])
    origin_flt_num = random.randint(1, 400)
    dest_flt_num = random.randint(1, 400)

    refund_data_list = [
        {
            "accountableDocumentNum": document_number,
            "issueLocalDate": datetime.datetime(issue_local_date.year, issue_local_date.month, issue_local_date.day),
            "customers": [
                {
                    "customerName": {
                        "firstName": faker.first_name(),
                        "lastName": faker.last_name()
                    },
                    "nameNum": "01.02",
                    "passengerTypeCode": passenger_type_code
                }
            ],
            "amounts": [
                {
                    "amountTypeCode": "TotalAmount",
                    "amount": {
                        "currencyEquivalentPrice": {
                            "decimalPrecisionCnt": 2,
                            "currencyAmt": round(random.uniform(1.00, 5000.00), 2),
                            "currencyCode": "USD"
                        }
                    }
                },
                {
                    "amountTypeCode": "RefundAmount",
                    "amount": {
                        "currencyEquivalentPrice": {
                            "decimalPrecisionCnt": 2,
                            "currencyAmt": round(random.uniform(1.00, 5000.00), 2),
                            "currencyCode": "USD"
                        }
                    }
                },
                {
                    "amountTypeCode": "RefundTotalTax",
                    "amount": {
                        "currencyEquivalentPrice": {
                            "decimalPrecisionCnt": 2,
                            "currencyAmt": round(random.uniform(1.00, 5000.00), 2),
                            "currencyCode": "USD"
                        }
                    }
                }
            ],
            "paymentMethod": [
                {
                    "paymentMethodTypeName": "CC",
                    "paymentCard": {
                        "paymentCardNetworkCode": random.choice(['AMEX', 'MC', 'VISA', 'DISC']),
                        "paymentCardNum": faker.credit_card_number()
                    }
                }
            ],
            "travelRelatedServiceTaxCategories": [
                {
                    "travelRelatedServiceTaxCategoryCode": "Refund",
                    "travelRelatedServiceTaxes": [
                        {
                            "chargeTypeCode": "AY",
                            "documentTaxFeeAmt": {
                                "currencyEquivalentPrice": {
                                    "decimalPrecisionCnt": 2,
                                    "currencyAmt": round(random.uniform(1.00, 100.00), 2),
                                    "currencyCode": "USD"
                                }
                            }
                        },
                        {
                            "chargeTypeCode": "XF",
                            "documentTaxFeeAmt": {
                                "currencyEquivalentPrice": {
                                    "decimalPrecisionCnt": 2,
                                    "currencyAmt": round(random.uniform(1.00, 100.00), 2),
                                    "currencyCode": "USD"
                                }
                            }
                        },
                        {
                            "chargeTypeCode": "ZP",
                            "documentTaxFeeAmt": {
                                "currencyEquivalentPrice": {
                                    "decimalPrecisionCnt": 2,
                                    "currencyAmt": round(random.uniform(1.00, 100.00), 2),
                                    "currencyCode": "USD"
                                }
                            }
                        }
                    ]
                }
            ],
            "couponList": [
                {
                    "couponNum": 1,
                    "couponStatusCode": 0,
                    "resequencedCouponNum": 101,
                    "ticketCouponSequenceNum": 1,
                    "accountableDocumentNum": document_number,
                    "originAirportCode": origin_code,
                    "destinationAirportCode": dest_code,
                    "flightNum": random.randint(1, 400),
                    "marketingFlightNum": origin_flt_num,
                    "operatedAsFlightNum": origin_flt_num,
                    "operatedAsCarrierCode": "DL",
                    "operatedAsCarrierName": "DL",
                    "operatedByFlightNum": origin_flt_num,
                    "scheduledDepartureLocalDate": datetime.datetime(origin_flt_date.year, origin_flt_date.month,
                                                                     origin_flt_date.day)
                },
                {
                    "couponNum": 2,
                    "couponStatusCode": 0,
                    "resequencedCouponNum": 102,
                    "ticketCouponSequenceNum": 2,
                    "accountableDocumentNum": document_number,
                    "originAirportCode": dest_code,
                    "destinationAirportCode": origin_code,
                    "flightNum": dest_flt_num,
                    "marketingFlightNum": dest_flt_num,
                    "operatedAsFlightNum": dest_flt_num,
                    "operatedAsCarrierCode": "DL",
                    "operatedAsCarrierName": "DL",
                    "operatedByFlightNum": dest_flt_num,
                    "scheduledDepartureLocalDate": datetime.datetime(return_flt_date.year, return_flt_date.month,
                                                                     return_flt_date.day)
                }
            ],
            "saleTypeCode": "D",
            "travelContextText": "Vol",
            "travelContextDesc": "Vol",
            "documentCategoryCode": "T",
            "refundType": "CC",
            "eligibility": {
                "refundable": True,
                "hasCompanion": True
            },
            "recordLocator": faker.iban(),
            "fareCalculation": {
                "fareCalculationLineText": faker.sentence(nb_words=10),
                "fareCalculationTypeCode": "N"
            }
        }
    ]

    return refund_data_list


####
# Generate the PNR segment
####
def get_pnr_segment(origin_airport_code, origin_ts, dest_airport_code, dest_ts):
    origin_departure_ts = datetime.datetime(origin_ts.year, origin_ts.month,
                                            origin_ts.day, origin_ts.hour, origin_ts.minute)

    dest_departure_ts = datetime.datetime(dest_ts.year, dest_ts.month,
                                          dest_ts.day, dest_ts.hour, dest_ts.minute)

    pnr_segment = [
        {
            "aircraftTypeCode": "DL",
            "cancelEligible": True,
            "destinationAirportCode": dest_airport_code,
            "flightSegmentNum": 1,
            "flown": False,
            "originAirportCode": origin_airport_code,
            "scheduledDepartureLocalTs": origin_departure_ts
        },
        {
            "aircraftTypeCode": "DL",
            "cancelEligible": True,
            "destinationAirportCode": origin_airport_code,
            "flightSegmentNum": 2,
            "flown": False,
            "originAirportCode": dest_airport_code,
            "scheduledDepartureLocalTs": dest_departure_ts
        }
    ]

    return pnr_segment


####
# Constants loaded from .env file
####
MDB_CONNECTION = settings.MDB_CONNECTION
MDB_DATABASE = settings.MDB_DATABASE
MDB_COLLECTION = settings.MDB_COLLECTION
NUM_DOCS = settings.NUM_DOCS

####
# Main
####
if __name__ == '__main__':
    main()

####
# Indicate end of script
####
end = timer()
endTs = time.localtime()
total_time = end - start

if total_time < 1:
    docs_inserted_time = int(NUM_DOCS) / 1
else:
    docs_inserted_time = int(NUM_DOCS) / total_time

print("\nEnding " + time.strftime("%Y-%m-%d %H:%M:%S", endTs))
print('====================================================')
print('Total Time Elapsed (in seconds): ' + str(total_time))
print('====================================================')
print('Number of Docs inserted per second: ' + str(docs_inserted_time))
print('====================================================')
