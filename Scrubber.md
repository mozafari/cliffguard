### Scrubber

TODO: description of how the scrubbing scripts work and how they are invoked

scrubber.py takes as input the stats.xml file and the out_dc_requests_issued file, and produces stats.xml-scrubbed and out_dc_requests_issued-scrubbed, such that the column names + strings in stats.xml-scrubbed are all scrubbed, and the column names + string constants in out_dc_requests_issued-scrubbed are also all scrubbed.

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
