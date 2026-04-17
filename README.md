# Teste Técnico - Engenheiro de Dados PySpark

## 📋 Contexto do Projeto

Este é um desafio técnico que simula um cenário real de análise de dados de um e-commerce. O projeto utiliza **Apache Spark** para processar dois conjuntos de dados principais:

- **Clientes**: ~10 mil registros de clientes do sistema
- **Pedidos**: ~1 milhão de registros de pedidos realizados

O objetivo é realizar análises de qualidade de dados, agregações, cálculos estatísticos e filtragens para entender o comportamento de compra dos clientes.

### Requisitos Técnicos

1. **Data Quality**: Identificar e reportar pedidos com problemas de qualidade
2. **Agregação**: Computar métricas por cliente (quantidade de pedidos, valor total)
3. **Análise Estatística**: Calcular média, mediana, percentis 10 e 90
4. **Filtragem - Acima da Média**: Listar clientes com valor total acima da média
5. **Filtragem - Média Truncada**: Listar clientes entre os percentis 10 e 90

---

## 📁 Organização das Pastas

```
test-tecnico-engenheiro-dados/
├── code/                          # Código-fonte principal
│   ├── config.py                 # Configurações compartilhadas
│   ├── main.py                   # Script de execução principal
│   ├── quality.py                # Regras de qualidade de dados
│   ├── repository.py             # Acesso aos dados (clientes e pedidos)
│   └── spark-warehouse/          # Diretório para metadados do Spark (local)
│
├── data/                          # Dados de entrada
│   ├── clients/
│   │   └── data.json            # Base de clientes (~10k registros)
│   └── pedidos/
│       └── data.json            # Base de pedidos (~1M registros)
│
├── instruções/
│   └── candidato.md             # Descrição do desafio e requisitos
│
└── tests/                         # Testes unitários
    ├── conftest.py              # Configurações e fixtures do pytest
    ├── test_config.py           # Testes do módulo config
    ├── test_main.py             # Smoke test do script principal
    ├── test_quality.py          # Testes das regras de qualidade
    └── test_repository.py       # Testes do módulo de acesso aos dados
```

### Descrição dos Módulos

| Módulo | Responsabilidade |
|--------|-----------------|
| `config.py` | Define o diretório base para resolver paths dos arquivos de entrada |
| `repository.py` | Carrega dados de clientes e pedidos com schemas explícitos |
| `quality.py` | Implementa regras de qualidade e gera relatórios de problemas |
| `main.py` | Orquestra a execução completa da análise |

---

## 🧪 Cobertura de Testes

O projeto inclui uma suíte de testes com **pytest** e**PySpark** para validar a lógica de processamento:

### Testes Implementados

| Arquivo | Descrição | Casos de Teste |
|---------|-----------|-----------------|
| `test_quality.py` | Valida a consolidação de IDs inválidos | 2 testes |
| `test_main.py` | Smoke test da execução principal | 1 teste |
| `conftest.py` | Fixtures compartilhadas | SparkSession local |

### Exemplos de Testes

1. **`test_get_invalid_order_ids_returns_union_of_quality_failures`**
   - Verifica se todos os IDs inválidos são consolidados
   - Testa detecção de: valores negativos,IDs nulos, clientes nulos, duplicatas

2. **`test_get_invalid_order_ids_deduplicates_repeated_failures`**
   - Garante que IDs inválidos apareçam apenas uma vez

3. **`test_main_executes_without_errors`**
   - Smoke test: valida se o script principal executa sem exceções

### Executar Testes

```bash
# Executar todos os testes
pytest tests/

# Executar testes com saída detalhada
pytest tests/ -v

# Executar testes de um módulo específico
pytest tests/test_quality.py -v

# Executar testes e mostrar cobertura
pytest tests/ --cov=code --cov-report=html
```

---

## 🚀 Como Executar o Código

### Pré-requisitos

- **Python 3.8+**
- **PySpark 3.x**
- **Dependências adicionais** (opcionais para desenvolvimento):
  - `pytest` - para executar testes
  - `pytest-cov` - para cobertura de testes

### Instalação de Dependências

```bash
# Instalar PySpark
pip install pyspark

# Instalar dependências de teste (opcional)
pip install pytest pytest-cov
```

### Executar o Script Principal

```bash
# Navegar até o diretório do código
cd code/

# Executar o script principal
python main.py
```

### Fluxo de Execução

O script `main.py` executa as seguintes operações em sequência:

1. **Carrega dados**: Lê clientes e pedidos dos arquivos JSON
2. **Valida qualidade**: Executa 5 verificações de qualidade:
   - Preços negativos
   - IDs de pedido nulos
   - IDs de cliente nulos
   - Pedidos vinculados a múltiplos clientes
   - IDs de pedido duplicados
3. **Filtra dados válidos**: Remove pedidos inválidos
4. **Agrega por cliente**: Calcula quantidade e valor total de pedidos
5. **Calcula estatísticas**: Média, mediana, percentis 10 e 90
6. **Exibe análises**:
   - Clientes com valor acima da média
   - Clientes entre percentis 10 e 90

### Saída Esperada

O script gera as seguintes saídas:

```
============================================

1. Pedidos com problemas de qualidade
============================================

[Tabela com ID do pedido e motivo do problema]
Problemas de qualidade encontrados: 

[Contagem por tipo de problema]

[Tabela com cliente, quantidade de pedidos e valor total]
Média aritmética do valor total de pedidos por cliente: [valor]
Mediana do valor total de pedidos por cliente: [valor]
Percentil 10: [valor]
Percentil 90: [valor]

Clientes com valor total de pedidos acima da média:
[Tabela com clientes]

Clientes com valor total de pedidos entre o percentil 10 e 90:
[Tabela com clientes]
```

---

## 🔍 Regras de Qualidade de Dados

O módulo `quality.py` implementa as seguintes validações:

| Regra | Descrição | Impacto |
|-------|-----------|--------|
| **Preço negativo** | `value < 0` | Pedido removido |
| **ID do pedido nulo** | `id IS NULL` | Pedido removido |
| **ID do cliente nulo** | `client_id IS NULL` | Pedido removido |
| **Pedido duplicado** | `id` aparece 2+ vezes | Ambas as ocorrências removidas |
| **Cliente múltiplo** | Mesmo `id` tem `client_id`s diferentes | Pedido removido |

---

## 📊 Estrutura de Dados

### Schema de Clientes

```
root
 |-- id: long (NOT NULL)
 |-- name: string (NOT NULL)
```

**Exemplo:**
```json
{"id": 1, "name": "João Silva"}
{"id": 2, "name": "Maria Santos"}
```

### Schema de Pedidos

```
root
 |-- id: long (NOT NULL)
 |-- value: double (NOT NULL)
 |-- client_id: long (NOT NULL)
```

**Exemplo:**
```json
{"id": 1, "value": 99.99, "client_id": 1}
{"id": 2, "value": 150.50, "client_id": 2}
```

---

## 💡 Notas de Implementação

- **Performance**: Utiliza operações nativas do Spark para otimizar o processamento de 1 milhão de registros
- **Memory**: A sessão Spark é configurada localmente (`local[1]`) por padrão
- **Logging**: Suprime logs verbosos do Spark para melhor legibilidade da saída
- **Persistência**: Os resultados são exibidos com `show()`, não persistidos em disco

---
