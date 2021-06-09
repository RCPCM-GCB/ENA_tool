# ENA_tool
Convenient dowloader of raw files for ENA.

- Fetches all samples info and metadata from xml files at ENA browser for a project by project id (accession). 
- Stores this information to `.csv` and `.html` files. 
- Allows to download .fastq files using [enaDataGet](https://github.com/enasequence/enaBrowserTools)

The examples of usage this tool and the outputs you can find in `examples` folder.

To use, please install:
 - EnaTool: `pip install ENATool`
 - enaDataGet: follow the [instructions](https://github.com/enasequence/enaBrowserTools) 

Before usage, please, make sure, that `enaDataGet` is installed properly and that command like `enaDataGet -f fastq -a ACCESSION` works fine.
