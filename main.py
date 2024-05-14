#Import bibliotecas usadas
import pandas as pd
from datetime import datetime, timedelta
import sqlite3

#Lendo csv em variavel separado por ","
funcionario = pd.read_csv('Funcionario.csv', sep=",")
cargo = pd.read_csv('Cargo1.csv', sep=",")
departamento = pd.read_csv('Departamento.csv', sep=",")
dependentes = pd.read_csv('Dependentes.csv', sep=",")
historicoSalario = pd.read_csv('HistoricoSalario.csv', sep=",")

#Conectando/Criando com banco de dados
aberturabancodedados = sqlite3.connect('bancodedados.db')
cursor = aberturabancodedados.cursor()

#Passando csv para Sql "Nome tabela, nome db, se existir substitua, não incluir indice"
funcionario.to_sql('funcionarios_sql',
                   aberturabancodedados,
                   if_exists='replace',
                   index=False)
cargo.to_sql('cargo_sql',
             aberturabancodedados,
             if_exists='replace',
             index=False)
departamento.to_sql('departamento_sql',
                    aberturabancodedados,
                    if_exists='replace',
                    index=False)
dependentes.to_sql('dependentes_sql',
                   aberturabancodedados,
                   if_exists='replace',
                   index=False)
historicoSalario.to_sql('historico_salario_sql',
                        aberturabancodedados,
                        if_exists='replace',
                        index=False)

# 1 Leitura por Python utilizando print
print("")
print("Dados questão 1")
print(funcionario)
print(cargo)
print(departamento)
print(dependentes)
print(historicoSalario)
print("")
#2
#Classe cursor ficam vinculado com a conexão
#Executamos o codigo sql selecionando nome,descriçao,nome do departamentos é nome dos dependentes , juntamos a tabela cargosql e comparamos o id do cargo com o id de cargo do funcionario mesma coisa com departamento e dependents , por fim ordenamos por nome do funcionario
cursor.execute('''
SELECT f.Nome AS funcionario, c.Descricao AS cargo, d.NomeDepartamento AS departamento, de.Nome AS dependentes 
FROM funcionarios_sql f 
INNER JOIN cargo_sql c ON f.CargoId = c.CargoID
INNER JOIN departamento_sql d ON f.DepartamentoId = d.CodigoDepartamento
LEFT JOIN dependentes_sql de ON f.FuncionarioId = de.FuncionarioId
ORDER BY de.Nome
''')
#Percorre um for , pegando todos os dados retornado pelo cursor com fetchall
for row in cursor.fetchall():
  print("")
  print("Dados questão 2 ")
  print(
      f"Funcionario: {row[0]}, Cargo: {row[1]}, Departamento: {row[2]}, Dependentes: {row[3]}"
  )

#3 Inicialmente verificamos se "HistoricoSalario esta vazio" com ajuda da importação datetime retornamos o valor de hoje e depois Calcula a data que estava três meses atrás a partir da data atual. depois converte a table "Mes" para datetime recebe salario de 3 mes atras verificando se e maior ou igual a 90 dias e se for maior ou igual a 90 dias ele retorna o salario agrupamos salario de 3 meses atras e salario atual , unimos a coluna  é recebemos exibimos o indice  comparamos se o aumento do salario é maior apos 90 dias verificamos se e vazio e retornamos print .
if not historicoSalario.empty:
  hoje = datetime.today()
  tres_meses_atras = hoje - timedelta(days=90)
  historicoSalario['Mes'] = pd.to_datetime(historicoSalario['Mes'])
  salarios_3_meses_atras = historicoSalario[historicoSalario['Mes'] >=
                                            tres_meses_atras]
  salarios_atuais = salarios_3_meses_atras.groupby(
      'FuncionarioId')['Salario'].last()
  df_salarios_funcionarios = pd.merge(funcionario,
                                      salarios_atuais,
                                      left_on='FuncionarioId',
                                      right_index=True)
  aumento_salarial = df_salarios_funcionarios[
      df_salarios_funcionarios['Salario_x'] <
      df_salarios_funcionarios['Salario_y']]
  print("")
  if not aumento_salarial.empty:
    print("Dados questão 3")
    print("Funcionários que tiveram aumento salarial nos últimos 3 meses:")
    print(aumento_salarial[['Nome', 'Salario_x', 'Salario_y']])
  else:
    print("Nenhum funcionário teve aumento salarial nos últimos 3 meses.")
else:
  print("Não há informações disponíveis no histórico de salários.")
print("")

#4 seleciona o nome do departamento e a media de idade dos dependente comparando ano de agora e ano de nascimento uni tabelas e agrupa em NomeDepartamento
cursor.execute('''
    SELECT NomeDepartamento, AVG(strftime('%Y', 'now') - strftime('%Y', d.DataNascimento)) AS MediaIdadeFilhos
    FROM funcionarios_sql f
    JOIN dependentes_sql d ON f.FuncionarioId = d.FuncionarioId
    JOIN departamento_sql dep ON f.DepartamentoId = dep.CodigoDepartamento
    GROUP BY NomeDepartamento
''')
resultados = cursor.fetchall()
for row in resultados:
  print("")
  print("Dados questão 4")
  print(f"Departamento: {row[0]} - Média de idade dos filhos: {row[1]}")

#5 Seleciona nome de dependentes e de funcionarios e verifica se e do cargo estagiario por id
cursor.execute('''
SELECT f.nome AS funcionario, d.nome AS dependentes
FROM funcionarios_sql f
JOIN dependentes_sql d ON f.FuncionarioId = d.FuncionarioId
WHERE f.CargoId = 1 OR f.CargoId = 2;
''')
for row in cursor.fetchall():
  print("")
  print("Dados questão 5")
  print(f"Nome do Estagiário: {row[0]} Nome do Filha(O):{row[1]}")

#6 Agrupa a media do do salario e nome do funcionarios , retorna a media maior por "idxmax" verifica se o id do funcionario e o mesmo do funcionario mais bem pago e retorna o print dele
media = historicoSalario.groupby('FuncionarioId')['Salario'].mean()
funcionariomaisbempg_id = media.idxmax()
funcionariomaisbempg = funcionario[funcionario['FuncionarioId'] ==
                                   funcionariomaisbempg_id]
print("")
print("Dados questão 6")
print("Funcionário com media mais bem pago:")
print(funcionariomaisbempg)

#7 função sql retorna nome de funcionario onde e pai de menina e analista compara o id do dependente com o da table funcionario e verifica um where para verificar se a descrição do cargo e analista e se seu grau de parentesco e filha , agrupa com group By e conta para verificar se e igual a duas "Filha" logo apos percorre um for no cursor, pegando todos
cursor.execute('''
SELECT f.Nome AS funcionario
FROM funcionarios_sql f
JOIN dependentes_sql d ON f.FuncionarioId = d.FuncionarioId
JOIN cargo_sql c ON f.CargoId = c.CargoId
WHERE c.CargoId IN (4, 5) AND d.Parentesco = 'Filha'
GROUP BY f.FuncionarioId
HAVING COUNT(*) = 2;
''')

# Obtendo todos os resultados da consulta
rows = cursor.fetchall()

print("")
print("Dados questão 7")
for row in rows:
  print("Analista pai de duas meninas:", row[0])

#8 Verifico se o cargoId se refere a analista logo apos faço a comparaçao se o seu Salario e maior ou igual a 5000 e menor que 9000, depois recolho oque retorno e busco o max , verifico com if se esta entre 5000 é 9000 e retorno o print
analistas = funcionario[(funcionario['CargoId'] == 3) |
                        (funcionario['CargoId'] == 4)]
analistas_salarios = analistas[(analistas['Salario'] >= 5000)
                               & (analistas['Salario'] <= 9000)]
analista_salario_maximo = analistas_salarios[
    analistas_salarios['Salario'] == analistas_salarios['Salario'].max()]
print("")
print("Dados questão 8")
if not analista_salario_maximo.empty:
  print("Analistas com o salário mais alto entre 5000 e 9000:")
  print(analista_salario_maximo)
else:
  print("Nenhum analista encontrado com salário entre 5000 e 9000.")

#9 seleciona nome de departamento , Count num de dependentes agrupa e ordena em ordem descresente armazena resultado em resultado e compara se vazio ou não
cursor.execute('''
    SELECT d.NomeDepartamento, COUNT(*) AS NumDependentes
    FROM departamento_sql d
    LEFT JOIN funcionarios_sql f ON d.CodigoDepartamento = f.DepartamentoId
    LEFT JOIN dependentes_sql dep ON f.FuncionarioId = dep.FuncionarioId
    GROUP BY d.NomeDepartamento
    ORDER BY NumDependentes DESC
''')
resultado = cursor.fetchone()
print("")
print("Dados questão 9")
if resultado:
  print("O departamento com o maior número de dependentes é:", resultado[0])
  print("Número de dependentes:", resultado[1])
else:
  print("Não há informações sobre dependentes nos departamentos.")

#10 a principel combinamos 2 table , e seguida utilizamos groupby e tiramos a media e deixamos claro que o e em ordem decrescente com "ascending=FALSE" a fim imprimimos o resultado
funcionario_com_departamento = pd.merge(funcionario,
                                        departamento,
                                        left_on='DepartamentoId',
                                        right_on='CodigoDepartamento')

media_salario_por_departamento = funcionario_com_departamento.groupby(
    'NomeDepartamento')['Salario'].mean().sort_values(ascending=False)
print("")
print("Dados questão 10")
print("Média de salário por departamento (em ordem decrescente):")
print(media_salario_por_departamento)
aberturabancodedados.close()
