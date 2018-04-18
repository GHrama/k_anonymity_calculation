def df_split(d, k_anon=1, debug=True):
    
    cities = ['Berlin', 'Hamburg', 'Frankfurt am Main',  'München',  'Düsseldorf', 'Stuttgart', 'Mannheim', 'Mainz', 'Darmstadt', 'Siegen']
    time_grouping = ['Hourly','Period','Daily']
    mot = ['With','Without']
    inn = pd.MultiIndex.from_product([mot, time_grouping], names=[ 'Mot', 'Aggregation'])
    sol = pd.DataFrame(np.zeros((6,10)), index=inn, columns=cities)
    #sol.loc[('Without', 'Hourly'),'Berlin']
    print("-----------------------------------------------------------------------------------------")
    print("NO-DAYS = {0}".format(k_anon))
    
    for index in range(0, len(cities)):
        r = d[(d['StartOrt']==cities[index]) & (d['EndOrt']==cities[index])]
        if r is not None:
            # convert dataframes
            # df with mot
            
            print("Length of df WITH MOT {0} for city {1}".format(r.shape, cities[index]))
            #print("WITH MOT")

            #print("HOURLY")
            #print(r.head())
            sol.loc[('With','Hourly'), cities[index]] = k_anonimity(r, no_days=k_anon)

            #print("PERIODICAL")
            r_period = convert_period(r)
            #print(r_period.head())
            sol.loc[('With','Period'), cities[index]] = k_anonimity(r_period, no_days=k_anon)

            #print("DAILY")
            r_daily = convert_daily(r)
            #print(r_daily.head())
            sol.loc[('With','Daily'), cities[index]] = k_anonimity(r_daily, no_days=k_anon)
            
            r_not_mot = without_mot(r)
            #print(r_not_mot)
            # convert dataframes
            # WITHOUT MOT
            
            print("Length of df with NO MOT is {0} for city {1}".format(r_not_mot.shape, cities[index]))
            #print("WITH MOT")

            #print("HOURLY")
            #print(r.head())
            sol.loc[('Without','Hourly'), cities[index]] = k_anonimity(r_not_mot, no_days=k_anon)

            #print("PERIODICAL")
            r_period_no_mot = convert_period_no_mot(r_not_mot)
            sol.loc[('Without','Period'), cities[index]] = k_anonimity(r_period_no_mot, no_days=k_anon)

            #print("DAILY")
            r_daily_no_mot = convert_daily_no_mot(r_not_mot)
            #print(r_daily.head())
            sol.loc[('Without','Daily'), cities[index]] = k_anonimity(r_daily_no_mot, no_days=k_anon)

    return sol
        
            
def convert_daily(d):
    d = d.drop(['HourOfDay'], axis=1)
    #print('Length before dropping {0}'.format(len(d)))
    dd = d.groupby(["StartId", "EndId","MoT"], as_index=False).sum()
    return dd

def convert_period(d):
    def tempFunc(x):
        number = x
        if int(number) in range(0, 6):
            #print(1)
            return "Night"
        elif int(number) in range(6, 10):
            #print(2)
            return "AmPeak"
        elif int(number) in range(10, 16):
            #print(3)
            return "MidDay"
        elif int(number) in range(16, 20):
            #print(4)
            return "PmPeak"
        elif int(number) in range(20, 24):
            #print(5)
            return "Evening"
        
    d['Period'] = d['HourOfDay'].apply(lambda x: tempFunc(x))
    dd = d.drop(['HourOfDay'], axis=1)
    #print('Length before dropping {0}'.format(len(d)))
    dd = dd.groupby(["StartId", "EndId","MoT", "Period"], as_index=False).sum()
    #print(dd.head())
    return dd

def convert_daily_no_mot(d):
    d = d[["StartId", "EndId", "Count"]]
    #print('Length before dropping {0}'.format(len(d)))
    dd = d.groupby(["StartId", "EndId"], as_index=False).sum()
    return dd

def convert_period_no_mot(d):
    def tempFunc(x):
        number = x
        if int(number) in range(0, 6):
            #print(1)
            return "Night"
        elif int(number) in range(6, 10):
            #print(2)
            return "AmPeak"
        elif int(number) in range(10, 16):
            #print(3)
            return "MidDay"
        elif int(number) in range(16, 20):
            #print(4)
            return "PmPeak"
        elif int(number) in range(20, 24):
            #print(5)
            return "Evening"
    #print(d)
    d['Period'] = d['HourOfDay'].apply(lambda x: tempFunc(x))
    dd = d[["StartId", "EndId", "Period","Count"]]
    print('Length before dropping {0} dd'.format(len(d)))
    #print(dd)
    dd = d.groupby(["StartId", "EndId", "Period"], as_index=False).sum()
    #print(dd.head())
    return dd

def k_anonimity(d,no_days=1):
    '''-> to multiply the counts by the number of day
       -> remove the counts below 5
       -> calculate original number of rows
       -> calculate the final number of rows
       -> caluclate %tage loss'''
    d['count_days'] = d['Count']*no_days
    #print(d.head())
    print('Original nunber of rows {0}'.format(d.shape))
    print('After k-anon number of rows {0}'.format(d[d['count_days']>=5].shape))
    print('Percentage of rows retained')
    percentage = (len(d[d['count_days']>=5]))*100/len(d)
    print(percentage)
    return float(percentage)


def check(d):
    cities = ['Berlin', 'Hamburg', 'Stuttgart', 'Frankfurt am Main',  'München',  'Düsseldorf', 'Stuttgart', 'Mannheim', 'Mainz', 'Darmstadt', 'Siegen']
    print("Rows with end and start in different cities? {0}".format(len(d[d['StartOrt'] != d['EndOrt']])))
    print("Cities not in the list ? {0}".format(len(d[~(d['StartOrt'].isin(cities) & d['EndOrt'].isin(cities))])))

def prelim(file):
    df_mot = pd.read_csv(file, delimiter="|")
    df_mot = df_mot.drop(['DayOfWeek'], axis=1)
    df_city = pd.read_csv("~/Downloads/zuordnung_plz_ort.csv")
    print('Length of the csv {0}'.format(len(df_mot)))
    print(df_mot.head())
    df_city = df_city.drop(['osm_id','bundesland'], axis=1)
    df_city = df_city.drop_duplicates(keep=False)
    merged = pd.merge(df_mot, df_city, left_on='StartId', right_on='plz')
    df_wanted = merged[merged['ort'].isin(['Berlin', 'Hamburg', 'Stuttgart', 'Frankfurt am Main',  'München',  'Düsseldorf', 'Stuttgart', 'Mannheim', 'Mainz', 'Darmstadt', 'Siegen'])]
    df_wanted.columns = ['StartId', 'EndId', 'MoT','HourOfDay', 'Count', 'StartOrt','plz']
    df_wanted = df_wanted.drop(['plz'], axis=1)
    merged1 = pd.merge(df_wanted, df_city, how='left', left_on='EndId', right_on='plz')
    merged1 = merged1.drop(['plz'], axis=1)
    merged1.columns = ['StartId', 'EndId', 'MoT','HourOfDay', 'Count','StartOrt', 'EndOrt']
    merged2 = merged1[merged1['StartOrt'] == merged1['EndOrt']]
    return merged2


def without_mot(d):
    dd = d.drop(['MoT'], axis=1)
    dd = d.groupby(['StartId','EndId', 'HourOfDay'],as_index=False).sum()
    return dd[['StartId','EndId','HourOfDay','Count']]
