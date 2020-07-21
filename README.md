Trabalho realizado com as Tecnologias: 

-> HDFS 

-> HIVE

-> Spark 


Proposta: 
 
Fazer análise dos resultados de partidas de futebol existentes no arquivo result.csv O dataset foi retirado do desafio existente no kaggel. https://www.kaggle.com/martj42/international-football-results-from-1872-to-2017/downloads/i nternational-football-results-from-1872-to-2017.zip/45 
 
Detalhe do arquivo 
 
Campo Arquivo 
Campo  Hive 
Tipo Descrição 
date Data date quando jogo ocorreu  
home_teamName  equipe_mandante  string equipe da casa 
away_teamName  equipe_visitante  string equipe visitante 
home_scoreHome  gol_equipe_mandante  int pontuação equipe da casa 
away_scoreAway  gol_equipe_visitante  int pontuação equipe visitante 
tournamentName  torneio  string torneio 
cityCity  cidade  string onde o jogo aconteceu 
countryCountry  pais  string onde a partida ocorreu 
neutralTRUE  fora_pais  boolean se o jogo ocorreu fora do país da equipe da casa, FALSE caso contrário 
 
  #Passos: 
  
  
  1- Importar o arquivo result.csv para o HDFS 
  
  2 - Criar uma tabela no Hive com o nome de ​resultado ​ , onde seja possível realizar consultas no arquivo result.csv. O nome das colunas devem respeitar o tipo e a nomenclatura informada acima. Executar uma query simples no HUE retornando as 5 primeiras linhas da tabela criada. 
  
  3 - Realizar análise utilizando Spark: AJUDA: copiar o hive-site.xml para o conf do Spark. sudo cp /etc/hive/conf/hive-site.xml /etc/spark/conf/ 
 
AJUDA: Para ler a tabela Hive e criar o dataframe, utilize o script abaixo: 
 
from pyspark.sql import HiveContext h = HiveContext(sc) df = h.sql("select * from default.resultado") 


#Após a criação do dataframe, responda as questões abaixo.  
 
a - quantos registro existem no dataframe

b - quantas equipes únicas mandantes existem no dataframe

c - quantas vezes as equipes mandantes saíram vitoriosas 

d - quantas vezes as equipes visitantes saíram vitoriosas

e - quantas partidas resultaram em empate 

f - a partir de um dataframe, crie uma tabela no Hive com o nome de partida_pais com o total de partida em cadas país 

g - utilizando o hive e a tabela partida_pais, em qual país teve mais partidas. 
