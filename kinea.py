from tabula import read_pdf
import urllib
from sqlalchemy import create_engine, Column, Integer, String, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

####Banco de dados##########

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                 "SERVER=DESKTOP-KSLPVKJ;"
                                 "DATABASE=Kinea;"
                                 "Trusted_Connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

Base = declarative_base()

#Classes que criam as tabelas e/ou colocam o dado no formato para serem inseridos no banco de dados

class Numeros_spdb(Base):
    __tablename__ = 'numeros_sp'
    id = Column(Integer, primary_key=True)
    date = Column(String)
    Vendas = Column(Numeric)
    Lancamentos = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_Mensal_porcentagem = Column(Numeric)
    VSO_Anual_porcentagem = Column(Numeric)
    VGV = Column(Numeric)

    def __repr__(self):
        return self.date
    
class Analise_seg_dorm_sp(Base):
    __tablename__ = 'seg_dorm_sp'
    id = Column(Integer, primary_key=True)
    Dormitorios = Column(String)
    Oferta_anterior = Column(Numeric)
    Lancamentos = Column(Numeric)
    Vendas = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_porcentagem = Column(Numeric)
    Data = Column(String)

    def __repr__(self):
        return self.Data
    
class Analise_seg_zona_sp(Base):
    __tablename__ = 'seg_zona_sp'
    id = Column(Integer, primary_key=True)
    Zona = Column(String)
    Oferta_anterior = Column(Numeric)
    Lancamentos = Column(Numeric)
    Vendas = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_porcentagem = Column(Numeric)
    Data = Column(String)

    def __repr__(self):
        return self.Data
    
class Analise_seg_area_sp(Base):
    __tablename__ = 'seg_area_sp'
    id = Column(Integer, primary_key=True)
    Area_util = Column(String)
    Oferta_anterior = Column(Numeric)
    Lancamentos = Column(Numeric)
    Vendas = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_porcentagem = Column(Numeric)
    Data = Column(String)

    def __repr__(self):
        return self.Data

class Analise_seg_preco_sp(Base):
    __tablename__ = 'seg_preco_sp'
    id = Column(Integer, primary_key=True)
    Preco = Column(String)
    Oferta_anterior = Column(Numeric)
    Lancamentos = Column(Numeric)
    Vendas = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_porcentagem = Column(Numeric)
    Data = Column(String)

    def __repr__(self):
        return self.Data
    
class Analise_seg_mercado_sp(Base):
    __tablename__ = 'seg_mercado_sp'
    id = Column(Integer, primary_key=True)
    Mercado = Column(String)
    Oferta_anterior = Column(Numeric)
    Lancamentos = Column(Numeric)
    Vendas = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_porcentagem = Column(Numeric)
    Data = Column(String)

    def __repr__(self):
        return self.Data

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

##############################

#IMPORTANTE:
#VGO pag 3

Ano = '2021'  # posso iterar entre anos
mes = '08'  # posso iterar por meses#
data = Ano+mes
dataformato = f'{mes}/{Ano}'
path = f'http://www.secovi.com.br/downloads/pesquisas-e-indices/pmi/{Ano}/arquivos/{data}-pmi.pdf'



def Numeros_sp(path):

    #retirando dados:
    # Vendas, lançamentos, oferta final, VSO (Mensal), VSO(12 meses), VGV

    vm = read_pdf(path, pages="1", area=(132.182, 11.694, 317.874, 156.988))
    vm = vm[0]
    vm.dropna(inplace=True)
    vm.rename(columns={'Números do mês:': 'Mes'}, inplace=True)
    vm = vm['Mes'].str.split(' = ', expand=True)
    vm.rename(columns={0: 'info', 1: f'{mes}/{Ano}'}, inplace=True)
    vm.set_index('info', inplace=True)
    vm.rename(index={'VSO (mensal)': 'VSO Mensal (%)'}, inplace=True)
    vm.rename(index={'VSO (12 meses)': 'VSO Anual (%)'}, inplace=True)

    #transformando o dado
    vm.replace(r'\.', '', regex=True, inplace=True) #comentar
    vm.replace(r'\,', '.', regex=True, inplace=True) #comentar
    vm.replace(r'[\sa-z-A-Zõã$%]', '', regex=True, inplace=True) #comentar

    # vm[f'{mes}/{Ano}'] = vm[f'{mes}/{Ano}'].str.replace('.','')
    # vm[f'{mes}/{Ano}'] = vm[f'{mes}/{Ano}'].str.replace(',','.')
    # vm[f'{mes}/{Ano}'] = vm[f'{mes}/{Ano}'].str.replace(r'[\sa-z-A-Zõã$%]','')
    vm.iloc[5] = round(float(vm.iloc[5]) * 1000000)
    vm = vm.T #comentar
    vm.index.name = "Mês"

    exists = session.query(Numeros_spdb).filter(Numeros_spdb.date == f"{mes}/{Ano}").all()
    if not exists:
        new_row = Numeros_spdb(date=f'{mes}/{Ano}', Vendas=float(vm.iloc[0][0]), Lancamentos=float(vm.iloc[0][1]), Oferta_final=float(
            vm.iloc[0][2]), VSO_Mensal_porcentagem=float(vm.iloc[0][3]), VSO_Anual_porcentagem=float(vm.iloc[0][4]), VGV=float(vm.iloc[0][5]))
        session.add(new_row)
        session.commit()
    else:
        print("mês já contabilizado")

    return vm

def Analise_segmentada_dormitorios_sp(path):

    if Ano > '2020' and data != "202101":
        #Retirando dados da análise segmentada por qtd de dormitórios em SP

        seg = read_pdf(path, pages="9", area=(73.356, 1.063, 185.338, 561.684))
        seg = seg[0]
        seg.set_index('Dormitórios', inplace=True)
        seg = seg.T
        seg = seg.drop('Total', 0)
        lista = [dataformato] * 4
        seg['Data'] = lista

        #transformando o dado

        seg.replace(r'\.', '', regex=True, inplace=True)
        seg.replace(r'\,', '.', regex=True, inplace=True)

        #adicionando no database
        exists = session.query(Analise_seg_dorm_sp).filter(
            Analise_seg_dorm_sp.Data == dataformato).all()
        if not exists:
            cont = 0
            for row, column in seg.iterrows():
                new_row = Analise_seg_dorm_sp(Dormitorios=seg.index[cont], Oferta_anterior=float(seg['Oferta anterior'][row]), Lancamentos=float(seg['Lançamentos'][row]), Vendas=float(
                seg['Vendas'][row]), Oferta_final=float(seg['Oferta final'][row]), VSO_porcentagem=float(seg['VSO (%)'][row]), Data=seg['Data'][row])
                session.add(new_row)
                session.commit()
                cont+= 1
        else:
            print("mês já contabilizado")
    return seg

def Analise_segmentada_zona_sp(path):

    #Retirando dados da análise segmentada por qtd de dormitórios em SP

    seg = read_pdf(path, pages="9", area=(194.552, 1.063, 309.369, 566.646))
    seg = seg[0]
    seg.set_index('Zona', inplace=True)
    seg = seg.T
    seg = seg.drop('Total', 0)
    lista = [dataformato] * seg.shape[0]
    seg['Data'] = lista

    #transformando o dado

    seg.replace(r'\.', '', regex=True, inplace=True)
    seg.replace(r'\,', '.', regex=True, inplace=True)
    
    exists = session.query(Analise_seg_zona_sp).filter(
        Analise_seg_zona_sp.Data == dataformato).all()
    if not exists:
        cont = 0
        for row, column in seg.iterrows():
            new_row = Analise_seg_zona_sp(Zona=seg.index[cont], Oferta_anterior=float(seg['Oferta anterior'][row]), Lancamentos=float(seg['Lançamentos'][row]), Vendas=float(
            seg['Vendas'][row]), Oferta_final=float(seg['Oferta final'][row]), VSO_porcentagem=float(seg['VSO (%)'][row]), Data=seg['Data'][row])
            session.add(new_row)
            session.commit()
            cont+= 1
    else:
        print("mês já contabilizado")

    return seg

def Analise_segmentada_area_util_sp(path):
    #Retirando dados da análise segmentada por zona em SP

    seg = read_pdf(path, pages="9", area=(320.001, 1.063, 450.411, 566.646))
    seg = seg[0]
    seg.set_index('Área útil (m2)', inplace=True)
    seg = seg.T
    seg = seg.drop('Total', 0)
    lista = [dataformato] * seg.shape[0]
    seg['Data'] = lista

    #transformando o dado

    seg.replace(r'\.', '', regex=True, inplace=True)
    seg.replace(r'\,', '.', regex=True, inplace=True)
    
    exists = session.query(Analise_seg_area_sp).filter(
        Analise_seg_area_sp.Data == dataformato).all()
    if not exists:
        cont = 0
        for row, column in seg.iterrows():
            new_row = Analise_seg_area_sp(Area_util=seg.index[cont], Oferta_anterior=float(seg['Oferta anterior'][row]), Lancamentos=float(seg['Lançamentos'][row]), Vendas=float(
            seg['Vendas'][row]), Oferta_final=float(seg['Oferta final'][row]), VSO_porcentagem=float(seg['VSO (%)'][row]), Data=seg['Data'][row])
            session.add(new_row)
            session.commit()
            cont+= 1
    else:
        print("mês já contabilizado")

    return seg

def Analise_segmentada_preco(path):
    #Retirando dados da análise segmentada por preço em SP

    seg = read_pdf(path, pages="9", area=(463.168, 0.354, 593.578, 566.646))
    seg = seg[0]
    seg.set_index('Preço (R$)', inplace=True)
    seg = seg.T
    seg = seg.drop('Total', 0)
    lista = [dataformato] * seg.shape[0]
    seg['Data'] = lista
    #transformando o dado

    seg.replace(r'\.', '', regex=True, inplace=True)
    seg.replace(r'\,', '.', regex=True, inplace=True)
    
    exists = session.query(Analise_seg_preco_sp).filter(
        Analise_seg_preco_sp.Data == dataformato).all()
    if not exists:
        cont = 0
        for row, column in seg.iterrows():
            new_row = Analise_seg_preco_sp(Preco=seg.index[cont], Oferta_anterior=float(seg['Oferta anterior'][row]), Lancamentos=float(seg['Lançamentos'][row]), Vendas=float(
            seg['Vendas'][row]), Oferta_final=float(seg['Oferta final'][row]), VSO_porcentagem=float(seg['VSO (%)'][row]), Data=seg['Data'][row])
            session.add(new_row)
            session.commit()
            cont+= 1
    else:
        print("mês já contabilizado")

    return seg

def Analise_segmentada_mercado(path):
    #Retirando dados da análise segmentada por mercado em SP

    seg = read_pdf(path, pages="9", area=(602.792, 1.063, 717.609, 565.937))
    seg = seg[0]
    seg.set_index('Mercado', inplace=True)
    seg = seg.T
    seg = seg.drop('Total', 0)
    lista = [dataformato] * seg.shape[0]
    seg['Data'] = lista

    #transformando o dado

    seg.replace(r'\.', '', regex=True, inplace=True)
    seg.replace(r'\,', '.', regex=True, inplace=True)
    
    exists = session.query(Analise_seg_mercado_sp).filter(
        Analise_seg_mercado_sp.Data == dataformato).all()
    if not exists:
        cont = 0
        for row, column in seg.iterrows():
            new_row = Analise_seg_mercado_sp(Mercado=seg.index[cont], Oferta_anterior=float(seg['Oferta anterior'][row]), Lancamentos=float(seg['Lançamentos'][row]), Vendas=float(
            seg['Vendas'][row]), Oferta_final=float(seg['Oferta final'][row]), VSO_porcentagem=float(seg['VSO (%)'][row]), Data=seg['Data'][row])
            session.add(new_row)
            session.commit()
            cont+= 1
    else:
        print("mês já contabilizado")

    return seg


numeros_do_mes = Numeros_sp(path)
por_segmento = Analise_segmentada_dormitorios_sp(path)
por_zona = Analise_segmentada_zona_sp(path)
por_area = Analise_segmentada_area_util_sp(path)
por_preco = Analise_segmentada_preco(path)
por_mercado = Analise_segmentada_mercado(path)

