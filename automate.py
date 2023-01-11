import os 


def download_dat_file(date):
    #C_VAR1_01012009_4.DAT
    new_date=date[6:8]+date[4:6]+date[:4]
    date=new_date 
    command='''wget -O /home/aman/eq_exchange_files/C_VAR1_'''+str(date)+'''_1.DAT --ftp-user=ftpguest --ftp-password='FTPGUEST' ftp://ftp.connect2nse.com/common/VARRate/C_VAR1_'''+str(date)+'''_1.DAT'''
    os.system(command)

def get_list_of_trades_date():
    path="/home/aman/daily_trades/eq_trades/2020"
    dir_list = os.listdir(path)
    date_list=[]

    for date in dir_list:
        date=date.replace("trades_","")
        date=date.replace("-","")
        #Select only EQ Dates
        if(date.startswith('eq_')):
            date=date.replace("eq_","")
            date_list.append(date)
    date_list.sort()
    return date_list

def is_DAT_file_exisits(date):
    new_date=date[6:8]+date[4:6]+date[:4]
    date=new_date
    dat_file_path="/home/aman/eq_exchange_files/C_VAR1_"+str(date)+"_1.DAT"
    if(os.path.exists(dat_file_path)):
        return True
    else:
        return False

#####  Driver Logic 
date_list=get_list_of_trades_date()
#date_list.remove('20170102')
for date in date_list:
    #check if DAT fileExists for that 
    if(not is_DAT_file_exisits(date)):
        print("Downloading DAT FILE FOR =>>",date)
        download_dat_file(date)
    print(date)
    os.system("python3 eq_span.py "+date)
    


