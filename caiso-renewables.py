import urllib
import datetime
import time
import os
import random
import glob
import pprint
import cPickle as pickle

def filenames():
    """Generate all filenames for CAISO renewables data up to yesterday."""
    today = datetime.date.today()
    d = datetime.date(2010, 4, 20) #oldest day recorded by CAISO
    while d < today:
        name = "{x}_DailyRenewablesWatch.txt".format(x=d.strftime('%Y%m%d'))
        yield name
        d += datetime.timedelta(1)
    

def main():
    q = []
    for x in filenames():
        if not os.path.exists(x):
            q.append(x)

    random.shuffle(q)
    while q:
        try:
            filename = q.pop()
            target = "http://content.caiso.com/green/renewrpt/" + filename
            print target
            data = urllib.urlopen(target).read()
            with open(filename, 'w') as outfile:
                outfile.write(data)

        except Exception, e:
            print Exception, e
            q.insert(0, filename)

        time.sleep(random.randint(1, 4))

    sum_energies()

def sum_energies():
    """Sum up all the renewable contributions to energy on each day."""

    columns_old = ['geothermal', 'biomass', 'biogas', 'small hydro',
                   'wind', 'solar']
    columns_new = ['geothermal', 'biomass', 'biogas', 'small hydro',
                   'wind', 'pv', 'solar thermal']
    
    inputs = glob.glob('*_DailyRenewablesWatch.txt')
    inputs.sort()
    totals = {}

    for fname in inputs:
        dt = fname[:4] + '-' + fname[4:6] + '-' + fname[6:8]
        sums = {}

        errors = ["[-11059] No Good Data For Calculation",
                  "Resize to show all values",
                  "Connection to the server lost.",
                  "#NAME?",
                  "Invalid function argument:Start time and End time differ by less than 15 micro seconds"]
        
        with open(fname) as infile:
            data = infile.read()

            #some files missing on server, but we downloaded 404 messages
            if '404 Not Found' in data:
                continue

            #newer files report solar pv and thermal separately
            if 'solar pv' in data.lower():
                columns = columns_new
            else:
                columns = columns_old

            #various error messages can appear instead of numerical megawatt
            #values -- replace them all with 0
            for e in errors:
                if e in data:
                    sums['ERROR'] = e
                    data = data.replace(e, '0')
            
            data = data.split('\n')[2:26]
            for line in data:
                energies = line.split()[1:]

                for k, v in enumerate(energies):
                    value = int(float(v))
                    try:
                        sums[columns[k]] += value
                    except KeyError:
                        sums[columns[k]] = value

            totals[dt] = sums

    with open('california-renewables.pkl', 'w') as outfile:
        outfile.write(pickle.dumps(totals))
        
    pprint.pprint(totals)

    winds = []
    solars = []
    monthlies = {}
    print "UNCONVENTIONALS"
    for k in sorted(totals.keys()):
        wind = totals[k]['wind']
        try:
            solar = totals[k]['solar thermal'] + totals[k]['pv']
        except KeyError:
            solar = totals[k]['solar']
        avg_power = (wind + solar) / 24.
        year, month, day = k.split('-')
        try:
            pwr, days = monthlies[(year, month)]
            pwr += avg_power
            days += 1
            monthlies[(year, month)] = (pwr, days)
        except KeyError:
            monthlies[(year, month)] = (avg_power, 1)
        print (k, str(solar), str(wind), "{0:.1f}".format(avg_power))
        solars.append(solar)
        winds.append(wind)

    yearlies = {}

    for k in sorted(monthlies.keys()):
        pwr, days = monthlies[k]
        avg_pwr = int(pwr / days)
        
        print("Monthly average power, MW", k, avg_pwr)
        try:
            yearlies[k[0]].append(avg_pwr)
        except KeyError:
            yearlies[k[0]] = [avg_pwr]

    for k in sorted(yearlies.keys()):
        avg_pwr = sum(yearlies[k]) / float(len(yearlies[k]))
        print("Yearly average power, MW", k, int(avg_pwr))
       
if __name__ == '__main__':
    main()
