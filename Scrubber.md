### Scrubber

TODO: description of how the scrubbing scripts work and how they are invoked

`scrubber.py` takes a statistics XML file and a query-log file, and produces scrubbed copies in which column names and string constants are sanitized.

You will also need beautiful soup installed [http://www.crummy.com/software/BeautifulSoup/](http://www.crummy.com/software/BeautifulSoup/)

You also need to have the lxml installed (it can serve as a tree builder for xml inputs) [http://lxml.de/installation.html](http://lxml.de/installation.html)

Note:
If lxml is giving you too much trouble, you can just replace the following line in scrubber.py:

```python
	stats_xml = BeautifulSoup(sfp, ['xml']) # requires lxml
```

with:

```python
	stats_xml = BeautifulSoup(sfp)
```
