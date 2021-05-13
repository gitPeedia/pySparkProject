## sparkProject
### Projeto focado em manipular e converter arquivos usando spark


### Usage Tasks
-   Make the git checkout after the fork
-   Paste the your source file folder to into 
the project on 'aux' path
-   Make the mapin of your fields into the aux/json
path (use the file estrutura_outra.json as example)
-   Change the main file to put the name of your folder
```python
table_name = "<<your_sub_path_name>>"
origin_path = "aux/orc/<<your_path_name>>/{}/".format(table_name)
target_path = "aux/csv/{}/".format(table_name)
```

### Run on container
-   Run the image canariodocker/pysparkproject using one volum to mapin your main file version and another to mapin your source folder as the exemple below
-   Now you are into the container at the python past. Execute the main file ```python3 main.py```

```bash
docker run -it -v "/home/<<your_user>>/<<your_folderr>>/pySparkProject/main.py:/spark-3.1.1-bin-hadoop2.7/python/main.py" -v "/home//home/<<your_user>>/<<your_folderr>>/pySparkProject/aux/orc:/spark-3.1.1-bin-hadoop2.7/python/aux/orc" --name pyspark canariodocker/pysparkproject:latest /bin/bash
```