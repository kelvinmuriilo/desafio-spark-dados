# Teste Técnico - Engenheiro de Dados PySpark

## Contexto

Este projeto resolve um desafio técnico de engenharia de dados com PySpark sobre uma base de clientes e pedidos de um e-commerce.

O pipeline faz:

1. leitura das bases JSON com schema explícito
2. validação de qualidade dos pedidos
3. exclusão dos pedidos inválidos da base analítica
4. agregação por cliente
5. cálculo de estatísticas sobre o valor total por cliente
6. filtragens pedidas no enunciado

## Estrutura do projeto

```text
desafio-spark/
├── data/
│   ├── clients/
│   │   └── data.json
│   └── pedidos/
│       └── data.json
├── instruções/
│   └── candidato.md
├── src/
│   ├── config.py
│   ├── main.py
│   ├── quality.py
│   └── repository.py
└── tests/
    ├── conftest.py
    ├── test_config.py
    ├── test_main.py
    ├── test_quality.py
    └── test_repository.py
```

## Módulos

- `src/config.py`: define `BASE_DIR` para resolver os caminhos do projeto.
- `src/repository.py`: lê clientes e pedidos com schema explícito.
- `src/quality.py`: monta o relatório consolidado de qualidade e extrai os IDs inválidos.
- `src/main.py`: orquestra a execução completa do pipeline e imprime os dataframes/resultados.

## Dados de entrada

### Clientes

Schema:

```text
root
 |-- id: long (nullable = false)
 |-- name: string (nullable = false)
```

### Pedidos

Schema:

```text
root
 |-- id: long (nullable = false)
 |-- value: decimal(5,2) (nullable = false)
 |-- client_id: long (nullable = false)
```

## Regras de qualidade implementadas

O relatório de qualidade marca pedidos com:

- valor do pedido negativo
- valor do pedido nulo
- `id` do pedido nulo
- `client_id` nulo
- `client_id` inexistente na base de clientes
- mesmo `id` de pedido vinculado a múltiplos clientes
- `id` duplicado

O dataframe de qualidade contém:

- `id`
- `motivo`

Além do relatório detalhado, o código também imprime um resumo por motivo com a coluna `Quantidade`.

## Saídas produzidas

O script principal exibe:

1. pedidos com problemas de qualidade
2. resumo das falhas por motivo
3. agregação por cliente com:
   - `client_id`
   - `nome_cliente`
   - `quantidade_pedidos`
   - `valor_total_pedidos`
4. estatísticas do valor total por cliente:
   - média aritmética
   - mediana
   - percentil 10
   - percentil 90
5. clientes com valor total acima da média
6. clientes com valor total entre os percentis 10 e 90

As saídas com `show()` foram configuradas para exibir até `100` linhas.

## Como executar

Pré-requisitos:

- Python 3.10+
- instalar as dependências do projeto

Instalação:

```bash
pip install -r requirements.txt
```

Execução do pipeline:

```bash
python3 src/main.py
```

## Testes

A suíte cobre os arquivos Python atuais do projeto:

- `test_config.py`: valida `BASE_DIR`
- `test_repository.py`: valida leitura, colunas e schemas
- `test_quality.py`: valida relatório de qualidade e consolidação de IDs inválidos
- `test_main.py`: smoke test do script principal e validação básica da saída textual

Executar todos os testes:

```bash
pytest tests/ -v
```

Executar com cobertura:

```bash
pytest tests/ --cov=src --cov-report=term-missing
```

## Observações de implementação

- o total por cliente é convertido para `decimal(11,2)`, como pedido no desafio
- os pedidos inválidos são removidos antes das agregações
- o join entre pedidos e clientes usa aliases explícitos para evitar ambiguidade no Spark 4
- as estatísticas exibidas são materializadas em um dataframe para manter a entrega alinhada ao enunciado
