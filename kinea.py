from tabula import read_pdf
import urllib
from sqlalchemy import create_engine, Column, Integer, String, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import warnings

warnings.filterwarnings("ignore")

####Banco de dados##########

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                 "SERVER=DESKTOP-KSLPVKJ;"
                                 "DATABASE=Kinea;"
                                 "Trusted_Connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

Base = declarative_base()

#Classes que criam as tabelas e/ou colocam o dado no formato para serem inseridos no banco de dados

class Numeros_mesdb(Base):
    __tablename__ = 'numeros'
    id = Column(Integer, primary_key=True)
    date = Column(String)
    Vendas = Column(Numeric)
    Lancamentos = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_Mensal_porcentagem = Column(Numeric)
    VSO_Anual_porcentagem = Column(Numeric)
    VGV = Column(Numeric)
    Local = Column(String)

    def __repr__(self):
        return self.date
    
class Analise_seg_dorm(Base):
    __tablename__ = 'seg_dorm'
    id = Column(Integer, primary_key=True)
    Dormitorios = Column(String)
    Oferta_anterior = Column(Numeric)
    Lancamentos = Column(Numeric)
    Vendas = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_porcentagem = Column(Numeric)
    Data = Column(String)
    Local = Column(String)

    def __repr__(self):
        return self.Data
    
class Analise_seg_zona(Base):
    __tablename__ = 'seg_zona'
    id = Column(Integer, primary_key=True)
    Zona = Column(String)
    Oferta_anterior = Column(Numeric)
    Lancamentos = Column(Numeric)
    Vendas = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_porcentagem = Column(Numeric)
    Data = Column(String)
    Local = Column(String)
    
    def __repr__(self):
        return self.Data
    
class Analise_seg_area(Base):
    __tablename__ = 'seg_area'
    id = Column(Integer, primary_key=True)
    Area_util = Column(String)
    Oferta_anterior = Column(Numeric)
    Lancamentos = Column(Numeric)
    Vendas = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_porcentagem = Column(Numeric)
    Data = Column(String)
    Local = Column(String)
    
    def __repr__(self):
        return self.Data

class Analise_seg_preco(Base):
    __tablename__ = 'seg_preco'
    id = Column(Integer, primary_key=True)
    Preco = Column(String)
    Oferta_anterior = Column(Numeric)
    Lancamentos = Column(Numeric)
    Vendas = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_porcentagem = Column(Numeric)
    Data = Column(String)
    Local = Column(String)
    
    def __repr__(self):
        return self.Data
    
class Analise_seg_mercado(Base):
    __tablename__ = 'seg_mercado'
    id = Column(Integer, primary_key=True)
    Mercado = Column(String)
    Oferta_anterior = Column(Numeric)
    Lancamentos = Column(Numeric)
    Vendas = Column(Numeric)
    Oferta_final = Column(Numeric)
    VSO_porcentagem = Column(Numeric)
    Data = Column(String)
    Local = Column(String)
    
    def __repr__(self):
        return self.Data

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

##############################

#IMPORTANTE:
#VGO pag 3

Ano = '2021'  # posso iterar entre anos
mes = '07'  # posso iterar por meses
data = Ano+mes
dataformato = f'{mes}/{Ano}'
path = f'http://www.secovi.com.br/downloads/pesquisas-e-indices/pmi/{Ano}/arquivos/{data}-pmi.pdf'



def Numeros_mes(path, pagina, local, area):

    #retirando dados:
    # Vendas, lançamentos, oferta final, VSO (Mensal), VSO(12 meses), VGV

    vm = read_pdf(path, pages=pagina, area=area) #seleciona o local a serem retiradas as informações
    vm = vm[0] #retira da lista gerada o dataframe que será utilizado
    vm.dropna(inplace=True) #retira do dataframe os valores em branco (no caso do texto provavelmente uma quebra de linha foi usada e é reconhecida como NaN = Not a number)
    vm.rename(columns={'Números do mês:': 'Mes'}, inplace=True) #renomeação da coluna para facilitar manipulação
    vm = vm['Mes'].str.split(' = ', expand=True) #o retorno é reconhecido como string, de um lado do que a informação trata e do outro o número de fato. É feita a separação tomando como base o sinal de igualdade presente em todas as strings.
    vm.rename(columns={0: 'info', 1: f'{mes}/{Ano}'}, inplace=True) #renomeação das colunas para facilitar a manipulação.
    vm.set_index('info', inplace=True) #transformo a primeira coluna no índice do dataframe.
    vm.rename(index={'VSO (mensal)': 'VSO Mensal (%)'}, inplace=True)  #índice renomeado
    vm.rename(index={'VSO (12 meses)': 'VSO Anual (%)'}, inplace=True) #índice renomeado
    
    #transformando o dado
    vm.replace(r'\.', '', regex=True, inplace=True) #tirando os "." das strings (esses pontos são separadores de milhar)
    vm.replace(r'\,', '.', regex=True, inplace=True) #trocando a "," (separador de casa decimal no nosso sistema) para ponto.
    vm.replace(r'[\sa-z-A-Zõã$%]', '', regex=True, inplace=True) #retirando das strings todo texto e símbolo além dos números e dos separadores decimais.
    vm.iloc[5] = round(float(vm.iloc[5]) * 1000000) # transformando de "x mil milhões" para bilhões
    vm = vm.T #o dataframe é uma matriz, o método .T é usado para transpor a matriz. Nesse caso, o objetivo é transformar a data como índice da tabela.
    vm.index.name = "Mês"

    #Teste para verificar a possibilidade de existência da informação coletada no banco. Caso exista, os dados não serão salvos.
    exists = session.query(Numeros_mesdb).filter(Numeros_mesdb.date == f"{mes}/{Ano}").all()
    if not exists:
        new_row = Numeros_mesdb(date=f'{mes}/{Ano}', Vendas=float(vm.iloc[0][0]), Lancamentos=float(vm.iloc[0][1]), Oferta_final=float(
            vm.iloc[0][2]), VSO_Mensal_porcentagem=float(vm.iloc[0][3]), VSO_Anual_porcentagem=float(vm.iloc[0][4]), VGV=float(vm.iloc[0][5]), Local=local)
        session.add(new_row)
        session.commit()
    else:
        print("mês já contabilizado")

    return vm

def Analise_segmentada_dormitorios(path, pagina, local, area):

    if Ano > '2020' and data != "202101":
        #Retirando dados da análise segmentada por qtd de dormitórios em SP

        seg = read_pdf(path, pages=pagina, area=area)
        seg = seg[0]
        seg.set_index('Dormitórios', inplace=True)
        seg = seg.T
        seg = seg.drop('Total', 0)
        lista = [dataformato] * seg.shape[0]
        lista2 = [local] * seg.shape[0]
        seg['Data'] = lista
        seg['Local'] = lista2

        #transformando o dado

        seg.replace(r'\.', '', regex=True, inplace=True)
        seg.replace(r'\,', '.', regex=True, inplace=True)

        #adicionando no database
        exists = session.query(Analise_seg_dorm).filter(
            Analise_seg_dorm.Data == dataformato).all()
        if not exists:
            cont = 0
            for row, column in seg.iterrows():
                new_row = Analise_seg_dorm(Dormitorios=seg.index[cont], Oferta_anterior=float(seg['Oferta anterior'][row]), Lancamentos=float(seg['Lançamentos'][row]), Vendas=float(
                seg['Vendas'][row]), Oferta_final=float(seg['Oferta final'][row]), VSO_porcentagem=float(seg['VSO (%)'][row]), Data=seg['Data'][row], Local=seg['Local'][row])
                session.add(new_row)
                session.commit()
                cont+= 1
        else:
            print("mês já contabilizado")
    return seg

def Analise_segmentada_zona(path, pagina, local, area):

    #Retirando dados da análise segmentada por qtd de dormitórios em SP

    seg = read_pdf(path, pages=pagina, area=area)
    seg = seg[0]
    seg.set_index('Zona', inplace=True)
    seg = seg.T
    seg = seg.drop('Total', 0)
    lista = [dataformato] * seg.shape[0]
    lista2 = [local] * seg.shape[0]
    seg['Data'] = lista
    seg['Local'] = lista2

    #transformando o dado

    seg.replace(r'\.', '', regex=True, inplace=True)
    seg.replace(r'\,', '.', regex=True, inplace=True)
    
    exists = session.query(Analise_seg_zona).filter(
        Analise_seg_zona.Data == dataformato).all()
    if not exists:
        cont = 0
        for row, column in seg.iterrows():
            new_row = Analise_seg_zona(Zona=seg.index[cont], Oferta_anterior=float(seg['Oferta anterior'][row]), Lancamentos=float(seg['Lançamentos'][row]), Vendas=float(
            seg['Vendas'][row]), Oferta_final=float(seg['Oferta final'][row]), VSO_porcentagem=float(seg['VSO (%)'][row]), Data=seg['Data'][row], Local=seg['Local'][row])
            session.add(new_row)
            session.commit()
            cont+= 1
    else:
        print("mês já contabilizado")

    return seg

def Analise_segmentada_area_util(path, pagina, local, area):
    #Retirando dados da análise segmentada por zona em SP

    seg = read_pdf(path, pages=pagina, area=area)
    seg = seg[0]
    seg.set_index('Área útil (m2)', inplace=True)
    seg = seg.T
    seg = seg.drop('Total', 0)
    lista = [dataformato] * seg.shape[0]
    lista2 = [local] * seg.shape[0]
    seg['Data'] = lista
    seg['Local'] = lista2

    #transformando o dado

    seg.replace(r'\.', '', regex=True, inplace=True)
    seg.replace(r'\,', '.', regex=True, inplace=True)
    
    exists = session.query(Analise_seg_area).filter(
        Analise_seg_area.Data == dataformato).all()
    if not exists:
        cont = 0
        for row, column in seg.iterrows():
            new_row = Analise_seg_area(Area_util=seg.index[cont], Oferta_anterior=float(seg['Oferta anterior'][row]), Lancamentos=float(seg['Lançamentos'][row]), Vendas=float(
            seg['Vendas'][row]), Oferta_final=float(seg['Oferta final'][row]), VSO_porcentagem=float(seg['VSO (%)'][row]), Data=seg['Data'][row], Local=seg['Local'][row])
            session.add(new_row)
            session.commit()
            cont+= 1
    else:
        print("mês já contabilizado")

    return seg

def Analise_segmentada_preco(path, pagina, local, area):
    #Retirando dados da análise segmentada por preço em SP

    seg = read_pdf(path, pages=pagina, area=area)
    seg = seg[0]
    seg.set_index('Preço (R$)', inplace=True)
    seg = seg.T
    seg = seg.drop('Total', 0)
    lista = [dataformato] * seg.shape[0]
    lista2 = [local] * seg.shape[0]
    seg['Data'] = lista
    seg['Local'] = lista2
    
    
    #transformando o dado

    seg.replace(r'\.', '', regex=True, inplace=True)
    seg.replace(r'\,', '.', regex=True, inplace=True)
    
    exists = session.query(Analise_seg_preco).filter(
        Analise_seg_preco.Data == dataformato).all()
    if not exists:
        cont = 0
        for row, column in seg.iterrows():
            new_row = Analise_seg_preco(Preco=seg.index[cont], Oferta_anterior=float(seg['Oferta anterior'][row]), Lancamentos=float(seg['Lançamentos'][row]), Vendas=float(
            seg['Vendas'][row]), Oferta_final=float(seg['Oferta final'][row]), VSO_porcentagem=float(seg['VSO (%)'][row]), Data=seg['Data'][row], Local=seg['Local'][row])
            session.add(new_row)
            session.commit()
            cont+= 1
    else:
        print("mês já contabilizado")

    return seg

def Analise_segmentada_mercado(path, pagina, local, area):
    #Retirando dados da análise segmentada por mercado em SP

    seg = read_pdf(path, pages=pagina, area=area)
    seg = seg[0]
    seg.set_index('Mercado', inplace=True)
    seg = seg.T
    seg = seg.drop('Total', 0)
    lista = [dataformato] * seg.shape[0]
    lista = [dataformato] * seg.shape[0]
    lista2 = [local] * seg.shape[0]
    seg['Data'] = lista
    seg['Local'] = lista2

    #transformando o dado

    seg.replace(r'\.', '', regex=True, inplace=True)
    seg.replace(r'\,', '.', regex=True, inplace=True)
    
    exists = session.query(Analise_seg_mercado).filter(
        Analise_seg_mercado.Data == dataformato).all()
    if not exists:
        cont = 0
        for row, column in seg.iterrows():
            new_row = Analise_seg_mercado(Mercado=seg.index[cont], Oferta_anterior=float(seg['Oferta anterior'][row]), Lancamentos=float(seg['Lançamentos'][row]), Vendas=float(
            seg['Vendas'][row]), Oferta_final=float(seg['Oferta final'][row]), VSO_porcentagem=float(seg['VSO (%)'][row]), Data=seg['Data'][row], Local=seg['Local'][row])
            session.add(new_row)
            session.commit()
            cont+= 1
    else:
        print("mês já contabilizado")

    return seg

#Dados São Paulo
numeros_do_mes = Numeros_mes(path, pagina="1", local = "São Paulo", area=(132.182, 11.694, 317.874, 156.988))
por_segmento = Analise_segmentada_dormitorios(path, pagina="9", local = "São Paulo", area=(73.356, 1.063, 185.338, 561.684))
por_zona = Analise_segmentada_zona(path, pagina="9", local = "São Paulo", area=(194.552, 1.063, 309.369, 566.646))
por_area = Analise_segmentada_area_util(path, pagina="9", local = "São Paulo", area=(320.001, 1.063, 450.411, 566.646))
por_preco = Analise_segmentada_preco(path, pagina="9", local = "São Paulo", area=(463.168, 0.354, 593.578, 566.646))
por_mercado = Analise_segmentada_mercado(path, pagina="9", local = "São Paulo", area=(602.792, 1.063, 717.609, 565.937))
#Dados Região Metropolitana
numeros_do_mes = Numeros_mes(path, pagina="10", local = "Região Metropolitana", area=)
por_segmento = Analise_segmentada_dormitorios(path, pagina="9", local = "Região Metropolitana", area=)
por_zona = Analise_segmentada_zona(path, pagina="9", local = "Região Metropolitana", area=)
por_area = Analise_segmentada_area_util(path, pagina="9", local = "Região Metropolitana", area=)
por_preco = Analise_segmentada_preco(path, pagina="9", local = "Região Metropolitana", area=)
por_mercado = Analise_segmentada_mercado(path, pagina="9", local = "Região Metropolitana", area=)
