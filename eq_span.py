import pandas as pd
from datetime import datetime, timedelta
import sys
import pymysql

def parse_postions(data):
    ''' In this Method we Will parse the Positions'''
    final_data=[]
    for k in data:
        for trade in data[k]:
            final_data.append(trade)
    postions=[]

    for trade in final_data:
        pos={}
        #Get Qty
        pos['qty']=trade[2]
        #Pos Name
        pos['posName']=trade[0]
        #tickerName
        pos['ticker']=trade[9]
        pos['price']=trade[3]
        pos['expo']=trade[7]
        #expo=>col trade[]
        postions.append(pos)
        
    return postions


def cumulate_data(lines):
    '''It will work as a Group By in Pandas by just combinnning similar trades
    Adding Subtracting there Quantity Acording to the need'''
    data={}
    for line in lines:
        if line[0]not in data:
            data[line[0]]=line
            #assign Expo
            data[line[0]][7]=data[line[0]][2]*data[line[0]][3]
            if(data[line[0]][1]=='S'):
                data[line[0]][7]=data[line[0]][7]*-1

            #print(line[0],line[6],data[line[0]][6])
        else:
            #Checking transaction Type
            #If Both same then add the Qty and and Average the price
            if line[1]==data[line[0]][1]:
                # Adding Qty
                # err-> avg price=(q1*p1+q2*p2)/(q1+q2)
                data[line[0]][2]=int(data[line[0]][2])+int(line[2])
                # Average the Price With New Formula ######
                #(q1p1 + q2p2)/(q1+q2)
                q1p1=data[line[0]][2]*data[line[0]][3]
                q2p2=line[2]*line[3]
                #Add Both Qty 
                q1q2=data[line[0]][2]+line[2]
                new_price=(q1p1+q2p2)/q1q2
                data[line[0]][3]= new_price
                #Assign the last time
                data[line[0]][5]=line[5]
                #Work on Expo -> 7
                if(line[1]=='S'):
                    data[line[0]][7]=data[line[0]][7]+(line[2]*line[3]*-1)
                else:
                    data[line[0]][7]=data[line[0]][7]+(line[2]*line[3])
            # If Both are Not same then
            # If in Map we have Buy and trade is For Sell
            if(data[line[0]][1]=='B' and line[1]=='S'):
                # Set T type if qty IS LARGER FOR Buy Set type-> Buy
                if int(data[line[0]][2])>int(line[2]):
                    data[line[0]][1]='B'
                else:
                    data[line[0]][1]='S'
                # Price average
                q1p1=data[line[0]][2]*data[line[0]][3]
                q2p2=line[2]*line[3]
                #Add Both Qty 
                q1q2=data[line[0]][2]+line[2]
                new_price=(q1p1+q2p2)/q1q2
                data[line[0]][3]= new_price
                #Qty if existing is larger
                if int(data[line[0]][2])>int(line[2]):
                    data[line[0]][2]=int(data[line[0]][2] ) -int(line[2])
                else:
                    data[line[0]][2]=int(line[2])-int(data[line[0]][2] )
                #Assign the last time
                data[line[0]][5]=line[5]
                #Work on Expo -> 7
                data[line[0]][7]=data[line[0]][7]+(line[2]*line[3]*-1)
            # If in Map we have Sell and trade is For Buy
            # 6->2  and 3->0 and 4->1  7->3
            if(data[line[0]][1]=='S' and line[1]=='B'):
                # if qty is larger for Sell Set Type -> Sell else Buy
                if int(data[line[0]][2])>int(line[2]):
                    data[line[0]][1]='S'
                else:
                    data[line[0]][1]='B'
                #Qty if existing is smaller than next then (next- existing Qty) else
                if data[line[0]][2]<int(line[2]):
                    data[line[0]][2]=int(line[2])-data[line[0]][2]
                else:
                    data[line[0]][2]=data[line[0]][2]-int(line[2])
                # Price average
                q1p1=data[line[0]][2]*data[line[0]][3]
                q2p2=line[2]*line[3]
                #Add Both Qty 
                q1q2=data[line[0]][2]+line[2]
                new_price=(q1p1+q2p2)/q1q2
                data[line[0]][3]= new_price
                #Assign the last time
                data[line[0]][5]=line[5]
                #Work on Expo -> 7
                data[line[0]][7]=data[line[0]][7]+(line[2]*line[3])
            #Assign the last IDX
            data[line[0]][6]=line[6]
            #print(line[0],line[6],data[line[0]][6])
    #Work on Negative Qty
    # ####################### IMpverify ! ####################################33333
    for k in data:
        if data[k][1]=='S':
            data[k][2]=data[k][2]*-1

    #Making it Ticker Wise 
    #print(data)
    alt_new_data={}
    for d in data:
        if data[d][9] not in alt_new_data:
            alt_new_data[data[d][9]]=[]
            alt_new_data[data[d][9]].append(data[d])
        else:
            alt_new_data[data[d][9]].append(data[d])

    return alt_new_data


def get_margin_percent(ticker):
    ticker_df=expo_file.loc[expo_file[1] == ticker]
    if(len(ticker_df)!=1):
        print("No Margin Numbers Found in C VAR File ...")
        quit()
    percent=ticker_df[9].values[0]
    return percent

def calcualte_expo(trades_map):
    for ticker in trades_map:
        ticker['qtyPrice']=abs(ticker['qty']*ticker['price'])
        ticker['tickerPercent']=get_margin_percent(ticker['ticker'])
        ticker['margin']=(ticker['qtyPrice']*ticker['tickerPercent']/100)
        ticker['expo']=(abs(ticker['expo'])*ticker['tickerPercent'])/100
    return trades_map


def write_to_db(trades_map,date,time,epoch,trader):
    '''Method to Write data to MYSQL DB'''
    print(trader,date,time)
    cursor = db.cursor()
    for ticker in trades_map:
        #print(ticker)
        command="Insert into eq_margin_granules  values ('"
        #(2022-11-01','09:17:03',8050037893','ALL','ticer',2,1,2,3,4,5,6,7,8)"'
        #print(trades_map[ticker].keys())
        final=command+str(date)+"','"+str(time)+"','"+str(int(epoch))+"',"+"'"+trader+"','"+str(ticker['ticker'])+"',"
        final=final+str(0)+","+str(0)+","
        final=final+str(ticker['expo'])+","+str(0)+","+str(ticker['expo'])+","
        final=final+str(ticker['tickerPercent'])+","+str(0)+","+str(0)+","+str(0)+");"
        cursor.execute(final)
        #cursor.execute("Insert into margin_granules  values ('2022-11-01','09:17:03',8050037893,'ALL','ticer',2,1,2,3,4,5,6,7,8)")
        db.commit()

def process(trader,temp_df,end_time):
    "This Method Is Used to call other Methods "
    print("Looking For =>",trader)
    list_of_trades= temp_df.values.tolist()
    #print(list_of_trades)
    dict_of_trades=cumulate_data(list_of_trades)
    pos=parse_postions(dict_of_trades)
    pos_with_expo=calcualte_expo(pos)
    # Writing To DB 
    date_time=datetime.fromtimestamp(end_time)
    print("Writing Db...",trader," at => ",date_time)
    write_to_db(pos_with_expo,date_time.date(),date_time.time(),end_time,trader)
    

#summation of (price * quantity) * last _column/100

#Get date as Sys Args 
sys.argv.pop(0)
if(not sys.argv):
    print("Please Enter Date To start the Script")
    quit()

date=sys.argv[0]
##### Db Connection 
####  Connect to the database
db = pymysql.connect(host='localhost',
                             user='aman',
                             password='aman@123',
                             database='margin_db',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
#########################
#########################
##### Read Trades File
new_date=date[:4]+"-"+date[4:6]+"-"+date[6:8]
trades_path="/home/aman/daily_trades/eq_trades/2020/eq_trades_"+new_date
trades_df = pd.read_csv(trades_path, header=None)
##########################
##### Read Exposure Percent file 
#Expo File Path 
dat_file_path="/home/aman/eq_exchange_files/"
new_date=date[6:8]+date[4:6]+date[:4]
temp_expo_file=pd.read_csv(dat_file_path+"C_VAR1_"+new_date+"_1.DAT", header=None,skiprows=[0])
expo_file=temp_expo_file[temp_expo_file[2]=="EQ"]
expo_file.reset_index(inplace=True,drop=True)
##########################
trades_df["date"] = pd.to_datetime(trades_df[4] + " " + trades_df[5])
trades_df["epoch"] = 0
trades_df["epoch"] = (trades_df["date"] - datetime(1970,1,1)).dt.total_seconds() - 19800
start_time = trades_df["epoch"][1]
end_time = start_time + 900
endoffile = trades_df["epoch"][len(trades_df) -1] + 900
while end_time < endoffile:
    traders=['All','PD', 'RY', 'AC', 'RL', 'SM', 'HM']
    #trader bases Calcualtion 
    for trader in traders:
        temp_df = trades_df.loc[trades_df["epoch"] < end_time]
    
        if(trader!="All"):
            temp_df=temp_df[temp_df[10]==trader]
            temp_df.reset_index(inplace=True,drop=True)
        if(len(temp_df)>0):
            process(trader,temp_df,end_time)
        
   
    end_time +=900
    