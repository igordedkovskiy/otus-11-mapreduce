# otus homework #11 - mapreduce

**Mapper and reducer for finding shortest prefix,**
**that uniquely identifies the string in the text**

## Usage

```bash
mapreduce <input-file> <num-of-mappers> <num-of-reducers>
```

* **input-file** contains the text to process
* **num-of-mappers** is the number of threads to execute map
* **num-of-reducers** is the number of threads to execute reduce

## Constraints

* **num-of-mappers >= 1**
* **num-of-reducers >= 1**
* **num-of-mappers >= num-of-reducers**