#ifndef CLIENTE_H
#define CLIENTE_H

int encurta(char *url_original, char *url_curta);
// Envia uma URL ao interceptador e retorna o código curto em url_curta.
// Retorna 0 em sucesso e -1 em falha.

int resolve(char *codigo_curto, char *url_original);
// Envia um código curto ao interceptador e retorna URL original.
// Retorna 0 em sucesso e -1 em falha.

int remove_url(char *codigo_curto);
// Remove um mapeamento.
// Retorna 0 em sucesso e -1 em falha.

#endif
