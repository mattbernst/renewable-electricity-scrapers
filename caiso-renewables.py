import urllib
import datetime
import time
import os
import random
import glob

class CAISO(object):
    def __init__(self):
        self.min_delay = 1
        self.max_delay = 4
        self.errors = ["[-11059] No Good Data For Calculation",
                       "Resize to show all values",
                       "Connection to the server lost.",
                       "#NAME?",
                       "#VALUE!",
                       "#REF!",
                       "Invalid function argument:Start time and End time differ by less than 15 micro seconds"]
    
    def filenames(self):
        """Generate all filenames for CAISO renewables data up to yesterday."""
        today = datetime.date.today()
        d = datetime.date(2010, 4, 20) #oldest day recorded by CAISO
        while d < today:
            name = "{x}_DailyRenewablesWatch.txt".format(x=d.strftime('%Y%m%d'))
            yield name
            d += datetime.timedelta(1)

    def daily_data(self):
        inputs = glob.glob("*_DailyRenewablesWatch.txt")
        inputs.sort()
        for fname in inputs:
            dt = fname[:4] + "-" + fname[4:6] + "-" + fname[6:8]
            with open(fname) as infile:
                data = infile.read()
                #some files missing on server, but we downloaded 404 messages
                if "404 Not Found" in data:
                    continue

                #various error messages can appear instead of numerical
                #megawatt values -- replace them all with 0
                for e in self.errors:
                    if e in data:
                        data = data.replace(e, '0')

                yield (dt, data)

    def main(self):
        q = []
        names = list(self.filenames())
        for x in names:
            if not os.path.exists(x):
                q.append(x)

        random.shuffle(q)
        while q:
            try:
                filename = q.pop()
                target = "http://content.caiso.com/green/renewrpt/" + filename
                print(target)
                data = urllib.urlopen(target).read()
                with open(filename, 'w') as outfile:
                    outfile.write(data)

            except Exception, e:
                print Exception, e
                q.insert(0, filename)

            time.sleep(random.randint(self.min_delay, self.max_delay))

        data_total = self.sum_all_energies()
        new_renewables = ["wind", "solar", "geothermal", "small hydro",
                          "biogas"]
        thermal_imports = ["thermal", "imports"]
        big_clean = ["nuclear", "hydro"]
        self.report(data_total, new_renewables)

    def sum_all_energies(self):
        columns_upper_old = ["geothermal", "biomass", "biogas", "small hydro",
                             "wind", "solar"]
        columns_upper_new = ["geothermal", "biomass", "biogas", "small hydro",
                             "wind", "pv", "solar thermal"]
        columns_lower = ["renewables", "nuclear", "thermal", "imports",
                         "hydro"]

        totals = {}

        for (dt, data) in self.daily_data():
            sums = {}

            #newer files report solar pv and thermal separately
            if "solar pv" in data.lower():
                columns_upper = columns_upper_new
            else:
                columns_upper = columns_upper_old

            #new-type renewables
            rdata = data.split('\n')[2:26]
            for line in rdata:
                energies = line.split()[1:]

                for k, v in enumerate(energies):
                    value = int(float(v))
                    try:
                        sums[columns_upper[k]] += value
                    except KeyError:
                        sums[columns_upper[k]] = value

                        cdata = data.split('\n')[30:54]

            #large hydro and other conventional sources
            for line in cdata:
                energies = line.split()[1:]
                for k, v in enumerate(energies):
                    value = int(float(v))
                    try:
                        sums[columns_lower[k]] += value
                    except KeyError:
                        sums[columns_lower[k]] = value

            totals[dt] = sums
        
        return totals

    def report(self, totals, components):
        monthlies = {}
        
        #Special case solar treatment to combine pv and solar thermal from
        #later reports, the way they are combined in early reports from the
        #server
        for k in sorted(totals.keys()):
            component_totals = []
            for j, component in enumerate(components):
                if component == 'solar':
                    try:
                        ctotal = totals[k]['solar thermal'] + totals[k]['pv']
                    except KeyError:
                        ctotal = totals[k]['solar']
                else:
                    ctotal = totals[k][component]
                component_totals.append(ctotal)
            avg_power = sum(component_totals) / 24.
            year, month, day = k.split('-')
            try:
                pwr, days = monthlies[(year, month)]
                pwr += avg_power
                days += 1
                monthlies[(year, month)] = (pwr, days)
            except KeyError:
                monthlies[(year, month)] = (avg_power, 1)
            print(k, dict(zip(components, component_totals)), "MW avg: %.1f" % avg_power)

        yearlies = {}

        for k in sorted(monthlies.keys()):
            pwr, days = monthlies[k]
            avg_pwr = int(pwr / days)

            print "Monthly power, MW", k, avg_pwr
            try:
                yearlies[k[0]].append(avg_pwr)
            except KeyError:
                yearlies[k[0]] = [avg_pwr]

        newestYear = max(yearlies.keys())
        newestYearMonths = len(yearlies[newestYear])

        for k in sorted(yearlies.keys()):
            avg_pwr = sum(yearlies[k]) / float(len(yearlies[k]))
            avg_pwr_ytd = sum(yearlies[k][:newestYearMonths]) / float(len(yearlies[k][:newestYearMonths]))
            print("Yearly average power, MW ({0} {1:.1f}) ({2} year to month {3:.1f})".format(k, avg_pwr, k, avg_pwr_ytd))

       
if __name__ == '__main__':
    scraper = CAISO()
    scraper.main()
