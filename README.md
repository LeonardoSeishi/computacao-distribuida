# Computação Distribuída

## T1 - Encurtador de URLs Distribuído

Serviços de encurtamento de URLs permitem transformar URLs longas em códigos
curtos e fáceis de compartilhar, além de possibilitar o rastreamento de acessos. Neste
trabalho, deve ser implementado um sistema de encurtamento de URLs simplificado, com
foco na comunicação distribuída entre componentes.

A arquitetura do sistema é composta por três elementos
principais:

**Clientes**, um **interceptador** (proxy) e um **servidor** REST (API alvo).
Os clientes se comunicam com o interceptador via **sockets** (usar API Berkley sockets) e o interceptador
repassa as requisições ao servidor via **API REST** (HTTP). O interceptador atua como um
**middleware**, podendo aplicar lógicas como cache, controle de tráfego ou tratamento de
falhas antes de encaminhar as requisições ao servidor.

### API Alvo (Servidor REST)

O servidor expõe uma API REST para gerenciamento de URLs encurtadas. É uma
aplicação independente que não possui conhecimento do interceptador: recebe requisições
HTTP e retorna respostas. O armazenamento dos mapeamentos (código curto -> URL
original) é feito localmente (em memória).

### Interceptador (Proxy)

O interceptador é o componente central do trabalho. Ele:

- Recebe requisições dos clientes via sockets TCP
- Processa as requisições aplicando padrões de projeto escolhidos (cache, circuit breaker, rate limiting, etc.)
- Repassa as requisições ao servidor via HTTP/REST
- Retorna as respostas ao cliente via sockets
- **O interceptador deve ser transparente para o servidor**, o servidor não sabe
que existe um proxy no meio. Do ponto de vista do cliente, o **interceptador é o
"servidor"**.

### Cliente

O cliente é um programa que se conecta ao interceptador via sockets TCP e envia
comandos para encurtar, resolver ou remover URLs. A API disponível ao cliente deve ser
oferecida por uma biblioteca, de modo que qualquer programador possa utilizar o serviço
importando a biblioteca.
Funções da biblioteca:

- `int encurta(char *url_original, char *url_curta)`:

Envia a URL original ao interceptador e recebe o código curto. Retorna código de erro em caso de falha.

- `int resolve(char *codigo_curto, char *url_original)`:

Envia o código curto ao interceptador e recebe a URL original. Retorna código de erro em caso de falha.

- `int remove_url(char *codigo_curto)`:

Remove o mapeamento de uma URL encurtada. Retorna código de erro em caso de falha.

#### Heterogeneidade

**Cliente:** Implementado em **Python** com biblioteca **C/C++**

### Coerência de cache no Interceptador

**Política de cache:**

O interceptador mantém um cache local dos mapeamentos de URLs resolvidas
recentemente (código curto -> URL original). Este cache utiliza o padrão Cache-Aside e tem
como objetivo reduzir requisições ao servidor REST, melhorando o tempo de resposta para
os clientes.
No caso de resoluções sucessivas, se o interceptador tiver uma cópia válida do
mapeamento em cache, a resposta é retornada diretamente ao cliente via socket, sem gerar
requisição ao servidor REST. Caso contrário (cache miss), o interceptador consulta o
servidor, armazena a resposta em cache e retorna ao cliente.
No caso de remoções, quando um cliente solicita a remoção de uma URL encurtada,
o interceptador encaminha a requisição DELETE ao servidor REST e invalida a entrada
correspondente no cache local, garantindo que consultas futuras não retornem dados
obsoletos.
A política de gerenciamento do cache (ex: LRU - Least Recently Used) e sua
capacidade máxima ficam a critério do grupo e devem ser configuráveis. É permitida a
utilização de estratégias complementares (fallback) como TTL (Time-to-Live) para expiração

### Configuração

`/config.txt`

Arquivo de configuração para inicializar o interceptador (endereço e a porta do servidor REST, o endereço e a porta do interceptador (para os clientes), e parâmetros do cache).

**Requisição no formato:**

```text

```

**Resposta:**

```text

```
