import uuid, argparse, json, csv, os
from collections import defaultdict

parser = argparse.ArgumentParser(description='')
parser.add_argument('--input', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

#uuid.uuid4()
youtube_users = {}
youtube_channels = {}

links = defaultdict(lambda: {})
for root, dirnames, filenames in os.walk(args.input):
  for filename in filenames:
    with open(os.path.join(root, filename), 'rb') as inputFile:
          for line in inputFile:
            try:
              record = json.loads(line[:-line[::-1].index('}')])
              if record['type'] == 'youtube_user':
                youtube_users[record['user']] = record
              elif record['type'] == 'youtube_channel':
                youtube_channels[record['channel']] = record
              elif record['type'] == 'link':
                links[record['Link']][record['pageURL']] = record
            except Exception as e:
              print e

linksfilename = args.output + '/links_' + str(uuid.uuid4())
link_output = []
print "Processing links"
for link, pageUrls in links.items():
  for pageUrl in pageUrls:
    link_output.append({'Link': link, 'pageUrl': pageUrl})
print "Writing links to ", linksfilename
with open(linksfilename , 'w') as linkFile:
  json.dump(link_output, linkFile, indent=4, separators=(',', ': '))

greaterthan50kfilename = args.output + '/youtube_greater_50k_' + str(uuid.uuid4())
lessthan50kfilename = args.output + '/youtube_less_50k_' + str(uuid.uuid4())


print 'Writing youtube subscriber data to', greaterthan50kfilename, 'and', lessthan50kfilename
with open(greaterthan50kfilename , 'w') as file50korgreater:
  with open(lessthan50kfilename , 'w') as filelessthan50k:
    greater_50k_writer = csv.writer(file50korgreater, delimiter=",", quotechar='"')
    less_50k_writer = csv.writer(filelessthan50k, delimiter=",", quotechar='"')

    for user, record in youtube_users.items():
      if (record['subscribers'] > 50000):
        greater_50k_writer.writerow(('user', record['user'], record['subscribers']))
      else:
        less_50k_writer.writerow(('user', record['user'], record['subscribers']))

    for channel, record in youtube_channels.items():
      if (record['subscribers'] > 50000):
        greater_50k_writer.writerow(('channel', record['channel'], record['subscribers']))
      else:
        less_50k_writer.writerow(('channel', record['channel'], record['subscribers']))
