
import re, gzip, boto, warc
import urlnorm, urlparse
from boto.s3.key import Key
from gzipstream import GzipStreamFile
from bs4 import BeautifulSoup
from collections import Counter
from mrjob.job import MRJob

domains_of_interest = [
"7eer.net", "a4dtrk.com", "anrdoezrs.net",
"api.shopstyle.com", "api.viglink.com", "apmebf.com", "aswalo.com",
"avantlink.com ", "awin1.com", "awopi.com", "baaloo.com", "bedop.com",
"beemrdwn.com", "blegu.com", "blesp.com", "boguys.com", "bpcvk.com",
"breju.com", "bremg.com", "bueraa.com", "bytemgdd.com", "byvue.com",
"camcil.com", "cjsab.com", "clapu.com", "click.linksynergy.com",
"clickbank.net", "cliop.com", "clixgalore.com ", "commissionfactory.com.au",
"commissionsoup.com", "copsil.com", "cullr.com", "derrt.com", "detyti.com",
"diloi.com", "dpbolvw.net", "dzxcq.com", "epartner.es", "ertile.com",
"eruity.com", "evyy.net", "flexlinks.com", "fobbel.com", "forgyi.com",
"fratic.com", "friuo.com", "gaesy.com", "gcnhu.com", "gdlckjoe.com",
"genpy.com", "gertiy.com", "gituy.com", "glopm.com", "gopjn.com",
"gotmos.com", "gretu.com", "grtdl.com ", "haramp.com", "hashof.com",
"hastrk1.com", "hastrk2.com", "hastrk3.com", "hurrum.com", "huyilo.com",
"hwoxt.com", "iexple.com", "ilcamc.com", "jdoqocy.com", "jeekl.com",
"jioet.com", "jiuyt.com", "jopik.com", "jursp.com", "kikuy.com",
"kiutho.com", "klert.com", "klirt.com", "klldabck.com", "kloiy.com",
"klopy.com", "kopsil.com", "kqzyfj.com", "krebe.com", "lewpp.com",
"limvus.com", "linkconnector.com", "lipsno.com", "lishus.com",
"loced.com", "lopley.com", "lxudv.com", "mb01.com ", "minuy.com",
"munop.com", "mutels.com", "naayna.com", "ojrq.net", "omgpm.com",
"omguk.com", "opiok.com", "partners.webmasterplan.com",
"peerfly.com", "pepperjamnetwork.com", "pfgbc.com", "phwqt.com",
"pjatr.com", "pjtra.com", "plecki.com", "pliok.com", "plipy.com",
"pntra.com", "pntrac.com", "pntrs.com", "pocves.com", "pohik.com",
"popshops.com", "prf.hn", "publicidees.co.uk", "publicidees.com",
"publicidees.es", "publicidees.it", "publicidees.net", "publicidees.nl",
"publicidees.pt", "puokie.com", "qksrv.net", "qnsr.com", "qouces.com",
"quiyp.com", "redirecting.at ", "redirectingat.com", "reussissonsensemble.fr",
"revenuewire.net ", "rewku.com", "rhkjp.com", "ringrevenue.com", "rstyle.me",
"samenresultaat.nl", "send.onenetworkdirect.net", "sigze.com", "smarttrk.com",
"straci.com", "successfultogether.co.uk", "supiy.com", "supiyl.com", "tihop.com",
"tkqlhce.com", "tradedoubler.com", "tradetracker.at", "tradetracker.be",
"tradetracker.bg", "tradetracker.ch", "tradetracker.cn", "tradetracker.co.uk",
"tradetracker.com", "tradetracker.com.au", "tradetracker.com.es", "tradetracker.de",
"tradetracker.dk", "tradetracker.es", "tradetracker.eu", "tradetracker.fi",
"tradetracker.fr", "tradetracker.hu", "tradetracker.in", "tradetracker.info",
"tradetracker.it", "tradetracker.jp", "tradetracker.mobi", "tradetracker.net",
"tradetracker.nl", "tradetracker.org", "tradetracker.ro", "treebe.com",
"trk2it1.com", "trk2it2.com", "trk2it3.com", "trk2it4.com", "ugosy.com",
"umthri.com", "uzjvh.com", "veduy.com", "vifils.com", "vokir.com",
"vrexot.com", "vutyls.com", "vuyti.com", "vykws.com", "warue.com",
"webgains.com", "wetrew.com", "whithe.com", "www.shareasale.com",
"xertp.com", "yupils.com", "zanox-affiliate.de", "zanox.com",
"zenaw.com", "zesep.com", "abctracx.com", "abebooks.com", "afcyhf.com",
"affiliatefuture.com", "avantlink.com", "belboon.com", "belboon.de",
"cardoffers.com", "cardsynergy.com", "clcktrack1.com", "clixgalore.com",
"commission-junction.com", "dbaff.com", "dgm-au.com", "dgtsls.com",
"digitalriver.com", "directtrack.com", "dpbird.com", "dtaffsys.com",
"dtlynx.com", "emjcd.com", "ftjcfx.com", "grtdl.com", "knjcdtrac.com", "lduhtrp.net",
"linkoffers.net", "linksynergy.com", "llmltrack1.com", "lnktrkx.com", "lynxtrack.com",
"mb01.com", "metaffiliation.com", "ncsreporting.com", "netaffiliation.be",
"netaffiliation.es", "netaffiliation.fr", "netaffiliation.it", "netaffiliation.nl",
"netaffiliation.pl", "newafftrac.com", "offershot.com", "onenetworkdirect.net",
"onlineshoes.com", "paidonresults.net", "qksz.net",
"redirecting.at", "revenuewire.net", "s2d6.com", "shareasale.com", "tqlkg.com"
]


HTML_TAG_PATTERN = re.compile('''<a(.*)?href=('|")?([^"'\s>]*)''')
YOUTUBE_SUBSCRIBER_PATTERN = re.compile('''subscription-button-subscriber-count[^>]*>([0-9,]*)<''')

def extract_links(body):
  links = []
  for link in HTML_TAG_PATTERN.findall(body):
    try:
      link = link[2]
      netloc = urlparse.urlparse(link).netloc
      if (netloc in domains_of_interest):
        link = urlnorm.norm(link)
        links.append(link)
    except:
      pass

  return links

def is_youtube_url(url):
  netloc = urlparse.urlparse(url).netloc
  if ('www.youtube.com' != netloc):
    return False

  if ('/user/' in url):
    return 'user'
  if ('/channel/' in url):
    return 'channel'

  return False

class TagCounter(MRJob):

  def mapper(self, _, line):
    f = None
    if True: #self.options.runner in ['emr', 'hadoop']:
      conn = boto.connect_s3(anon=True)
      pds = conn.get_bucket('aws-publicdatasets')
      k = Key(pds, line)
      f = warc.WARCFile(fileobj=GzipStreamFile(k))
    else:
      print 'Loading local file {}'.format(line)
      f = warc.WARCFile(fileobj=gzip.open(line))

    for i, record in enumerate(f):
      for key, value in self.process_record(record):
        yield key, value
      self.increment_counter('commoncrawl', 'processed_records', 1)

  def combiner(self, key, value):
    yield key, sum(value)

  def reducer(self, key, value):
    yield key, sum(value)


  def process_record(self, record):

    if record['Content-Type'] == 'application/http; msgtype=response':
      payload = record.payload.read()

      headers, body = payload.split('\r\n\r\n', 1)
      body = unicode(body.lower().lower(), 'utf-8', errors='replace')

      if 'Content-Type: text/html' in headers:
        links = extract_links(body)
        for link in links:
          yield {'type' : 'link', 'Link' : link, 'pageURL' : record.url}, 1

        if is_youtube_url(record.url) == 'user':
          user, sub = get_youtube_sub(record.url, 'user', body)
          if user and sub:
            yield {'type' : 'youtube_user', 'user' : user, 'subscribers' : sub}, 1

        if is_youtube_url(record.url) == 'channel':
          channel, sub = get_youtube_sub(record.url, 'channel', body)
          if channel and sub:
            yield {'type' : 'youtube_channel', 'channel' : channel, 'subscribers' : sub}, 1

        self.increment_counter('commoncrawl', 'processed_pages', 1)

def get_youtube_sub(url, id_type, body):
  num_sub = YOUTUBE_SUBSCRIBER_PATTERN.findall(body)
  if len(num_sub) == 1:
    url_seg = url.replace('?', '/').split('/')
    id = url_seg[url_seg.index(id_type) + 1]
    return (id, int(num_sub[0].replace(',', '')))
  return (None, None)

if __name__ == '__main__':
  TagCounter.run()
