import pandas as pd
import fxcmpy
import numpy as np
from pandas.tseries.offsets import BDay


class connect_fxcm:
    '''

    Conexão com a corretora FXCM conta demo

    '''
    def __init__(self, token):

        '''

        :param token: Token de acesso Vindo pego na conta demo da corretora

        '''

        self.ACCESS_TOKEN = token
        self.con = fxcmpy.fxcmpy(access_token=self.ACCESS_TOKEN, log_level='error')
        print(self.con.is_connected())
        self.con_connect = self.con.is_connected()
        self.con_id = self.con.get_account_ids()
        self.instruments = self.con.get_instruments()

        if self.con_connect:
            print('A conexão foi um sucesso,\n O ID é {}'.format(self.con_id))
            print('Os Ativos Disponivels são:\n', self.instruments)
        else:
            print('O resultado da conexão foi {}'.format(self.con_connect))

        self.offers = self.con.get_offers()

        self.df = pd.DataFrame(self.offers)

        print(self.df[['currency', 'offerId']])
        self.lista_tg = ['m1', 'm5', 'm15', 'm30', 'H1', 'H2', 'H3', 'H4', 'H6', 'H8', 'D1', 'W1']
        self.sem_horas = self.lista_tg[-2:]

    def criacao_DF(self, cod, period, n_candles):


        self.period = period
        self.cod = cod
        self.candles = self.con.get_candles(cod, period=period, number=n_candles)
        self.candles['open'] = self.candles[['bidopen', 'askopen']].mean(axis=1)
        self.candles['high'] = self.candles[['bidhigh', 'askhigh']].mean(axis=1)
        self.candles['low'] = self.candles[['bidlow', 'asklow']].mean(axis=1)
        self.candles['close'] = self.candles[['bidclose', 'askclose']].mean(axis=1)

        self.candles = self.candles[['open', 'high', 'low', 'close', 'tickqty']]
        self.candles.rename(index=str, columns={'tickqty': 'AskVol'}, inplace=True)
        print(self.candles.head())

        return self.df_wrangler(self.candles, self.period)

    def df_wrangler(self, df, tempo_grafico):

        """

        :param df: Dataframe com os preços, para poder fazer o ETL
        :return: Retorna o DataFrame ja feito o datawrangler apropriado para o forex
        """

        self.df = df.set_index(pd.to_datetime(df.index))
        # df.drop('date', axis=1, inplace=True)
        # self.df.index = pd.to_datetime(df.index, format='%Y.%m.%d %H:%M:%S.%f')
        self.isBussinessDay = BDay().onOffset
        self.match_series = pd.to_datetime(self.df.index).map(self.isBussinessDay)
        self.df = self.df[self.match_series]

        """
        if tempo_grafico in self.sem_horas:
            self.date = [d.date() for d in self.df.index]
            print(self.df.head())
        """
        self.df.to_csv('/home/jair_rapids/Documentos/GBP_USD_corretora.csv')
        return self.df
