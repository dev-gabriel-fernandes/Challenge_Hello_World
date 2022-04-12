# Contexto - Hello World!
- A empresa HW (Hello World) fornece um sistema de pagamentos *white label* para seus clientes (chamados de *ISOs*: *Independent Sales Operator* - Operador de vendas independente), que, por sua vez, podem, por exemplo, disponibilizar maquininhas para sua rede de vendedores (*merchants* - comerciantes). 
- A HW possui 4 grandes clientes (ISOs) em setores diferentes: venda de eletrodomésticos, rede de restaurantes por quilo, venda de cookies e venda de cimento. 
- Os serviços da HW geram juntos mais de 10GB de dados por dia. 
- Você é o mais novo membro da equipe de Plataforma de Dados da HW, que é responsável pela disponibilização de dados a todos da empresa HW para análise.
# Etapa 1
Você recebeu uma amostra do banco de dados da HW para que possa tratá-la de maneira adequada. 

- Essa é uma amostra da tabela de transações, que será utilizada para futuras análises na HW (*transactions.csv*). 
- Além dela, você também tem acesso à tabela de bandeira do cartão (*card\_brand.csv*), com a relação de/para do código com o nome da bandeira. 

**Objetivo:**

Você deve criar um (ou mais) script (de preferência em Python) para tratar essa base para que a equipe de negócio possa analisá-la com menor esforço possível.



Sobre a tabela de *transactions*:


|**CAMPO**|**DESCRIÇÃO**|
| :- | :- |
|id|Identificação da transação (interno)|
|created\_at|Data e hora em que a transação foi criada no sistema HW|
|merchant\_id|Identificação do comerciante correspondente à transação|
|valor|Montante da transação, em centavos de reais|
|n\_PARCELAS|Número de parcelas em que a transação foi feita|
|Nome\_no\_Cartao|Nome registrado no cartão|
|status|Status da transação no sistema HW|
|card\_id|Identificador do cartão em que a transação foi efetuada|
|iso\_id|Identificação do ISO correspondente à transação|
|card\_brand|Código da bandeira do cartão em que a transação foi efetuada|
|DOCUMENTO|CPF vinculado ao cartão|

Sobre a tabela *card\_brands*:


|**CAMPO**|**DESCRIÇÃO**|
| :- | :- |
|id|Identificação do registro da bandeira do cartão (interno)|
|brand\_name|Nome da bandeira do cartão|
|brand\_code|Código da bandeira do cartão|

