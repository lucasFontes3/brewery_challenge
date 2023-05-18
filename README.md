Segue desafio resolvido. Importante citar alguns pontos:

 - Ao trabalhar com pyspark utilizo sempre a versão free, o databricks community, e essa versão me impossibilita de orquestrar jobs usando o workflow.
 - O código em pyspark criado funciona via databricks, mas dada a impossibilidade de orquestrar, optei por usar o Astro SDK da Airflow(que roda usando docker)
 - O código no airflow foi feito porém dado que não tenho ambiente spark local não consegui testar 
 - A parte de particionar por location não ficou clara pra mim, então particionei por cidade.
 - Aproveito para passar meu [portfolio](https://lucasbfontes.github.io/), tem 2 projetos que eu uso pyspark em batch, e no outro uso como streaming.
 - Segue imagem contendo print da camada gold

![Screen Shot 2023-05-18 at 10 48 55](https://github.com/lucasFontes3/brewery_challenge/assets/133979797/67efcd20-cf0d-475b-bb53-140a8734d334)
