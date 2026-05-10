#include <stdio.h>
#include "cliente.h"

int main() {
    char codigo[1024];
    char url[1024];

    if (encurta("https://www.google.com", codigo) == 0) {
        printf("Encurtado: %s\n", codigo);
    } else {
        printf("Erro ao encurtar URL\n");
        return 1;
    }
    if (resolve(codigo, url) == 0) {
        printf("URL original: %s\n", url);
    } else {
        printf("Erro ao resolver URL\n");
        return 1;
    }
    if (remove_url(codigo) == 0) {
        printf("Removido com sucesso!\n");
    } else {
        printf("Erro ao remover URL\n");
        return 1;
    }

    return 0;
}
