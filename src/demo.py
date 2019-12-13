import glob
import luigi
from luigi.contrib.spark import PySparkTask, SparkSubmitTask


class GenerateCountryListTask(PySparkTask):
    task_namespace = "examples"
    name = "Generate_Country_List_App"
    app = "generate_country_list_spark.py"

    def output(self):
        return luigi.LocalTarget("/tmp/demo/country_list/")

    def app_options(self):
        return []

class FilterCountryData(PySparkTask):

    task_namespace = "examples"
    name = "Filter_Country_Data_App"
    app = "filter_country_data_spark.py"

    country = luigi.Parameter(default=str)

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget("/tmp/demo/{}/raw".format(self.country))

    def app_options(self):
        return [self.country]


class GenerateCountryRevenue(PySparkTask):

    task_namespace = "examples"
    name = "Generate_Country_Revenue_App"
    app = "generate_country_revenue_spark.py"

    country = luigi.Parameter(default=str)

    def requires(self):
        return [FilterCountryData(country=self.country)]

    def output(self):
        return luigi.LocalTarget("/tmp/demo/{}/revenue".format(self.country))

    def app_options(self):
        return [self.country]


class GenerateAllCountriesRevenue(luigi.Task):
    task_namespace = "examples"

    def requires(self):
        requires_list = []
        countries_list = list()
        with open('/home/kjoshi/luigi-demo/luigi-sandbox/src/countries_list.txt') as countries_file:
            countries_list = list(countries_file)

        for country in countries_list:
            requires_list.append(GenerateCountryRevenue(country=country.strip()))

        return requires_list

    def output(self):
        return luigi.LocalTarget("/tmp/demo/all_countries/")

    def run(self):
        print("Generate Revenues!")


if __name__ == "__main__":
        luigi.run(['examples.GenerateAllCountriesRevenue', '--workers', '4'])
