import numpy as np
import pandas as pd
import datetime as dt
import time
import re
import functions.utils_db_alch2 as db

import os
import csv
import subprocess


class AxiomaModel:
    # Absolute path to risk model
    _direc_rm = ['C:', 'Axioma', 'AxiomaAPI_2015R1.4', 'riskmodelextract']
    # Absolue root path of output
    _direc_out_root = 'C:\\AxiomaRMOutput'
    # relative path to output files (from root)
    _direc_out = ['TMP']
    # relative path t0 final files (from root)
    _direc_fin = ['riskmodels']

    def __init__(self, modelDate=None, model='WW21AxiomaMH', offline=True,
                 check_existing_csv=True):
        """
        "modelDate is None"
        modelDate is None & offline=True & check_existing_csv=True
        > Find's Most Recent Date, checks if offline exists, if not create new
        modelDate is None & offline=True & check_existing_csv=False
        > Find's Most Recent Date, if offline exists, find nearest offline file

        "modelDate is not None"
        modelDate not None & offline=True & check_existing_csv=True
        > Check offline files, but if csv doesn't exist, create new
        modelDate not None & offline=True & check_existing_csv=False
        > Check offline files, if doesn't exist, find nearest offline file
        """
        self.requestDate = modelDate
        self.riskmodel = model
        conn = db.connectDB("MSSQL_Axioma")
        if modelDate is None:
            if offline:
                if not check_existing_csv:
                    self.modelDate = self.last_offline_RM()
                    conn.close()
                else:
                    Sql_S = "Select convert(char(8), max(dt), 112) as BASE_DT\n"
                    Sql_S += "  from axiomadb_kic_2015r2.dbo.model_factor_{}".format(model[:7].lower())
                    cursor = conn.cursor()
                    cursor.execute(Sql_S)
                    self.modelDate = cursor.fetchval()
                    conn.close()
            else:
                Sql_S = "Select convert(char(8), max(dt), 112) as BASE_DT\n"
                Sql_S += "  from axiomadb_kic_2015r2.dbo.model_factor_{}".format(model[:7].lower())
                cursor = conn.cursor()
                cursor.execute(Sql_S)
                self.modelDate = cursor.fetchval()
                conn.close()
        else:
            if offline:
                if not check_existing_csv:
                    self.modelDate = self.last_offline_RM(date=self.requestDate)
                    conn.close()
                else:
                    Sql_S = "Select convert(char(8), max(dt), 112) as BASE_DT\n"
                    Sql_S += "  from axiomadb_kic_2015r2.dbo.model_factor_{}\n".format(model[:7].lower())
                    Sql_S += " where dt <= '{}'".format(modelDate)
                    cursor = conn.cursor()
                    cursor.execute(Sql_S)
                    self.modelDate = cursor.fetchval()
            else:
                Sql_S = "Select convert(char(8), max(dt), 112) as BASE_DT\n"
                Sql_S += "  from axiomadb_kic_2015r2.dbo.model_factor_{}\n".format(model[:7].lower())
                Sql_S += " where dt <= '{}'".format(modelDate)
                cursor = conn.cursor()
                cursor.execute(Sql_S)
                self.modelDate = cursor.fetchval()
                conn.close()

    def RM_Extract_raw(self):
        cwd = '/'.join(self._direc_rm)
        exe, arg0, arg1, arg2 = (
            'RiskModelExtract.bat',
            'dates.txt', self.riskmodel,
            self._direc_out_root + '\\' + '\\'.join(self._direc_out)
        )
        Arg_lst = [cwd + '/' + exe, arg0, arg1, arg2]

        with open('/'.join(self._direc_rm) + '/dates.txt', 'w') as file:
            csv_writer = csv.writer(file, delimiter=',', lineterminator='\n')
            csv_writer.writerow([self.modelDate])

        self.execute_batch(Arg_lst, cwd)

    def RM_Extract_sedol(self):
        arg2 = self._direc_out_root + '\\' + '\\'.join(self._direc_out)
        if not os.path.exists(arg2):
            os.mkdir(arg2)
        file_lst = [f for f in os.listdir(arg2) if os.path.isfile(arg2 + '\\' + f)]  # files in target folder
        file_ = '.'.join([self.riskmodel, self.modelDate])  # filename
        chk = [file_ + '.' + ext for ext in ['exp', 'rsk', 'cov']]
        if not all([file in file_lst for file in chk]):
            self.RM_Extract_raw()

        self.getAxiomaIdMap()
        raw = pd.read_csv(arg2 + '\\' + file_ + '.exp', delimiter='|', header=0,
                          skiprows=[1], index_col=0, low_memory=False)
        raw.index.name = 'AxiomaID'
        map_ = self.sedol_map.loc[self.sedol_map['ax_DT'] == self.modelDate,
                                  ['AxiomaID', 'SEDOL']
                                  ].set_index('AxiomaID')['SEDOL']
        map_.dropna(inplace=True)
        sedol_exp = pd.concat([raw, map_], join='inner', axis=1).set_index(
            'SEDOL', drop=True)
        sedol_exp.drop('USD', axis=1, inplace=True)

        raw = pd.read_csv(arg2 + '\\' + file_ + '.rsk', delimiter='|', header=0,
                          skiprows=[1, 2], index_col=0, low_memory=False)
        raw.index.name = 'AxiomaID'
        sedol_rsk = pd.concat([raw, map_], join='inner', axis=1).set_index(
            'SEDOL', drop=True)

        f_cov = pd.read_csv(arg2 + '\\' + file_ + '.cov', delimiter='|', header=0,
                            index_col=0, low_memory=False)
        f_cov.drop('USD', axis=1, inplace=True)
        f_cov.drop('USD', axis=0, inplace=True)
        f_cov, sedol_exp = self.AlignColumns(f_cov, sedol_exp)

        arg3 = self._direc_out_root + '/' + '/'.join(self._direc_fin)
        sedol_exp.to_csv(arg3 + '/' + file_ + '_exp.csv')
        sedol_rsk.to_csv(arg3 + '/' + file_ + '_rsk.csv')
        f_cov.to_csv(arg3 + '/' + file_ + '_cov.csv')

    def get_RM(self):
        arg3 = self._direc_out_root + '/' + '/'.join(self._direc_fin)
        if not os.path.exists(arg3):
            os.mkdir(arg3)
        file_lst = [f for f in os.listdir(arg3) if os.path.isfile(arg3 + '\\' + f)]  # files in target folder
        file_ = '.'.join([self.riskmodel, self.modelDate])  # filename
        chk = [file_ + ext for ext in ['_exp.csv', '_rsk.csv', '_cov.csv']]
        if not all([file in file_lst for file in chk]):
            self.RM_Extract_sedol()

        self.factor_exp = pd.read_csv(
            arg3 + '/' + file_ + '_exp.csv', header=0, index_col=0, low_memory=False)
        self.factor_rsk = pd.read_csv(
            arg3 + '/' + file_ + '_rsk.csv', header=0, index_col=0, low_memory=False)
        self.factor_cov = pd.read_csv(
            arg3 + '/' + file_ + '_cov.csv', header=0, index_col=0, low_memory=False)
        self.factors_main = self.factor_exp.columns[self.factor_exp.apply(lambda x: len(x.unique()) > 2)]

    def last_offline_RM(self, date=None):
        arg3 = self._direc_out_root + '/' + '/'.join(self._direc_fin)
        dates = {f.split('.')[1][:8] for f in os.listdir(arg3) if os.path.isfile(arg3 + '\\' + f)}
        if date is None:
            return max(dates)
        else:
            return max([x for x in dates if x <= date])

    def getAxiomaIdMap(self):
        Sql_S = " Select convert(char(8), ma.dt, 112) as ax_DT,                  "
        Sql_S += "        a.primary_symbol as AxiomaID,                           "
        Sql_S += "        b.market_sedol as SEDOL,                                "
        Sql_S += "        b.market_ticker as Ticker                               "
        Sql_S += " from axiomadb_kic_2015r2.dbo.market_asset_1 ma with (nolock),  "
        Sql_S += "      axiomadb_kic_2015r2.dbo.asset a  with (nolock) ,          "
        Sql_S += "      axiomadb_kic_2015r2.dbo.identifier_asset_1 b with (nolock)"
        Sql_S += " where ma.dt in ('{}')      ".format(self.modelDate)
        Sql_S += "   and   a.asset_id = ma.asset_id                           "
        Sql_S += "   and   b.asset_id = ma.asset_id                           "
        Sql_S += "   and   b.dt = ma.dt                                       "
        Sql_S += " order by ax_DT, a.primary_symbol                           "
        conn = db.connectDB("MSSQL_Axioma")
        self.sedol_map = pd.read_sql(Sql_S, conn)
        conn.close()

    @staticmethod
    def execute_batch(Arg_lst, cwd=None):
        prt = subprocess.check_output(Arg_lst, cwd=cwd, shell=True)
        prt_lst = str(prt).split('\\n')
        output = [line_.replace('\\t', '\t') for line_ in prt_lst]
        for line in output:
            print(line)

    @staticmethod
    def AlignColumns(cov, exp):
        """Align Cov, Exp by Column Order of Covariance Matrix"""

        elimChar = '[-&,.() ]'  # Regular Expression
        cov_col = pd.Series(cov.columns)
        cov_col = cov_col.str.replace(elimChar, '')
        cov.columns = cov_col
        cov_idx = pd.Series(cov.index)
        cov_idx = cov_idx.str.replace(elimChar, '')
        cov.index = cov_idx
        cov = cov.loc[cov_col, cov_col]
        exp_col = pd.Series(exp.columns).str.replace(elimChar, '')
        exp.columns = exp_col
        exp = exp[cov_col]

        return cov, exp


if __name__ == '__main__':
    A = AxiomaModel('20171229')
    A.get_RM()
