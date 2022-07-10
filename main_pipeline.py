import analysis_pipeline as ap
import share_strategy as sp
import coin_strategy as cp
import datetime
import time

if __name__ == "__main__":
    #last_hour = -1
    while True:
        #hour = datetime.datetime.now().hour
        #if last_hour != hour:
            print("###################################################")
            print(f"starting pipelines at: {datetime.datetime.now()}")
            print("###################################################")

            try:
                ap.pipeline()
            except:
                print("ANALYSIS PIPELINE FAILED")
            
            try:
                sp.run_strategy()
            except:
                print("SHARE PIPELINE FAILED")

            try:
                cp.run_strategy()
            except:
                print("COIN PIPELINE FAILED")

        #    last_hour = hour
            print("###################################################")
            print(f"pipelines finished at: {datetime.datetime.now()}")
            print("###################################################")
        
        #time.sleep(300)

            time.sleep(3600 - time.time() % 3600)