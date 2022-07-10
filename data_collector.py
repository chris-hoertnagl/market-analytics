import coin_data_collector as cdc
import share_data_collector as sdc
import pmetals_data_collector as pdc

if __name__ == "__main__":
    coin_data_collector = cdc.LiveDataCollector()
    coin_data_collector.start()
    print("###########################################")
    print("########## coin collector started #########")
    print("###########################################")

    share_data_collector = sdc.LiveDataCollector()
    share_data_collector.start()
    print("###########################################")
    print("########## share collector started #########")
    print("###########################################")

    pmetal_data_collector = pdc.LiveDataCollector()
    pmetal_data_collector.start()
    print("###########################################")
    print("########## share collector started #########")
    print("###########################################")