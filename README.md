# Workers Consumers Raims

Esta pasta centraliza os workers de processamento assíncrono do Raims.

Os workers existem para retirar tarefas pesadas, contínuas ou desacopladas da API principal, permitindo que o sistema processe eventos em segundo plano com mais segurança, escalabilidade e organização.

## Objetivo

Os workers desta pasta são responsáveis por processamentos como:

- consumo de eventos
- leitura de filas
- persistência assíncrona
- centralização de logs
- integração com Redis
- integração com Kafka
- envio de e-mails
- processamento desacoplado de rotinas de suporte

## Estrutura Utilizada

Cada worker deve seguir, preferencialmente, a mesma organização base:

- `Program.cs`
  Responsável pelo bootstrap do worker, carregamento de `appsettings.json`, configuração de DI, logging e registro dos serviços.

- `Worker.cs`
  Responsável pelo fluxo principal do `BackgroundService`, escuta da fila, loop de processamento e ciclo de vida do worker.

- `Options/`
  Classes de configuração mapeadas a partir do `appsettings.json`.

- `Services/`
  Serviços internos do worker, como integração com Redis, Kafka, SMTP, renderização de templates e regras de processamento.

- `Models/`
  Contratos internos, eventos, DTOs e objetos auxiliares usados no processamento.

- `Templates/`
  Pasta opcional para templates de e-mail e arquivos relacionados ao conteúdo enviado pelo worker.

- `appsettings.json`
  Arquivo único de configuração do worker. Toda configuração operacional do projeto deve ficar aqui.

## Workers Atuais

### `WorkerLogs`

Responsável por consumir logs centralizados no Kafka e persisti-los em disco.

Função principal:

- escutar o tópico `application.logs` no Kafka
- processar lotes de logs
- gravar arquivos de log em disco
- transformar logs estruturados em linhas legíveis para auditoria operacional

Uso no ecossistema:

- centraliza erros e eventos operacionais em um único fluxo
- desacopla a gravação de logs da aplicação principal e dos workers
- reduz custo de I/O direto nos processos produtores
- permite processamento em lote

### `WorkerMail`

Responsável por consumir pedidos de envio de e-mail, processar o evento e realizar o disparo.

Função principal:

- escutar eventos no Kafka
- publicar erros operacionais no `Core.Log`
- validar o payload recebido
- garantir idempotência com Redis
- controlar concorrência entre múltiplos workers
- renderizar templates de e-mail
- enviar e-mail via SMTP
- encaminhar falhas permanentes para DLQ

Uso no ecossistema:

- desacopla o envio de e-mail da API principal
- evita travar fluxos síncronos por causa de SMTP
- permite escalar múltiplas instâncias consumidoras
- reduz risco de duplicidade com controle de processamento

## Convenções Deste Diretório

- cada worker deve ser isolado em sua própria pasta
- cada worker deve possuir seu próprio `appsettings.json`
- integrações externas devem ser encapsuladas em `Services`
- configurações não devem ficar hardcoded no código quando forem operacionais
- contratos de eventos devem ficar claros e rastreáveis
- logs importantes devem ser publicados no fluxo central de `Core.Log`
- processamento concorrente deve considerar idempotência e condição de corrida

## Fluxo Esperado

De forma geral, o padrão de funcionamento dos workers é:

1. A API ou outro serviço publica um evento.
2. O worker consumidor escuta a fila ou tópico correspondente.
3. O worker valida e processa a mensagem.
4. O worker executa a ação necessária.
5. Em caso de falha permanente, o evento pode ser enviado para tratamento separado, como DLQ.

## Observação

Esta pasta deve servir como padrão para novos workers do Raims. Sempre que um novo consumidor for criado, ele deve seguir a mesma linha estrutural para manter consistência, manutenção simples e previsibilidade entre projetos.
