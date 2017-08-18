# --function-name scrapeReddit

# set global variables (build and runtime)
config = {
    'function_name': 'scrapeReddit',
    'source_url': 'https://api.bitcoincharts.com/v1/csv/',
    'db': 'reddit',
    'url': 'https://api.coinmarketcap.com/v1/ticker/?limit=1',
    'fresh_build': True,
    'clear_build': True
}
rds = {
    'host': 'crypto-data.crapipl8g1ks.us-east-2.rds.amazonaws.com',
    'user': 'shiraz',
    'passwd': 'crypto_rds',
    'db': 'crypto_data',
    'connect_timeout': 5,
    'table_name': 'coinmarketcap'
}
log = ['Running ' + config['function_name']]

def lambda_handler(event, context):
    import json
    import praw
    import datetime
    import requests
    import operator
    from firebase import firebase
    fb = firebase.FirebaseApplication('https://crypto-trading-953aa.firebaseio.com/', None)
    r = praw.Reddit(client_id='SsQqcWQP5RuOXQ', client_secret='usg_tSv9niczDt4Jya4fB8lmdhw', user_agent='fractal_bot')

    def getFbKeys(b=''):
        url = 'https://crypto-trading-953aa.firebaseio.com/' + b + '.json?shallow=true&print=pretty'
        # try:
        keys = json.loads(requests.get(url).content).keys()
        # except:
        #     keys = []
        return keys

    def textToWords(text):
        # text = text.replace('1', ' 1 ')
        # text = text.replace('2', ' 2 ')
        # text = text.replace('3', ' 3 ')
        # text = text.replace('4', ' 4 ')
        # text = text.replace('5', ' 5 ')
        # text = text.replace('6', ' 6 ')
        # text = text.replace('7', ' 7 ')
        # text = text.replace('8', ' 8 ')
        # text = text.replace('9', ' 9 ')
        # text = text.replace('0', ' 0 ')
        text = text.replace('!', ' ')
        text = text.replace('@', ' ')
        text = text.replace('#', ' ')
        text = text.replace('$', ' ')
        text = text.replace('%', ' ')
        text = text.replace('^', ' ')
        text = text.replace('&', ' ')
        text = text.replace('*', ' ')
        text = text.replace('(', ' ')
        text = text.replace(')', ' ')
        text = text.replace('_', ' ')
        text = text.replace('+', ' ')
        text = text.replace('-', ' ')
        text = text.replace('=', ' ')
        text = text.replace('[', ' ')
        text = text.replace(']', ' ')
        text = text.replace('{', ' ')
        text = text.replace('}', ' ')
        text = text.replace('|', ' ')
        text = text.replace("'", '')
        text = text.replace('"', ' ')
        text = text.replace(':', ' ')
        text = text.replace(';', ' ')
        text = text.replace('/', ' ')
        text = text.replace('?', ' ')
        text = text.replace('.', ' ')
        text = text.replace(',', ' ')
        text = text.replace('<', ' ')
        text = text.replace('>', ' ')
        text = text.replace('\\', ' ')
        text = text.lower()
        text = text.split()
        return text

    def getWordCount(words, wc={}):
        for w in words:
            if w not in wc:
                wc[w] = 1
            else:
                wc[w] += 1
        return wc

    def filterWordCount(wc):
        wc_filtered = {}
        for w in wc:
            if wc[w] > 1:
                wc_filtered[w] = wc[w]
        return wc_filtered

    def interestingFilter(text):
        articles = 'a an the one some few this it your '
        pronouns = 'i my you im '
        conjunctions = 'and to in on beside for nor but or yet so because if when as of is with at '
        conjunctions += 'like do how what are from got get have be can that was did about not its does '
        skip = articles + pronouns + conjunctions
        # skip = '. the to of ? a ! , - is in that 1 2 3 4 5 6 7 8 9 0 '
        # skip += 'you and on : be this my we for with not will i me x '
        # skip += 'are just / at @ what it $ from how have ( ) do about '
        # skip += 'or can an when all why if some like so by has your '
        # skip += 'any get one going know was again more as way [ ] into '
        # skip += 'after says s us u its '
        return [w for w in text.split() if w not in skip.split()]

    def main():
        start = datetime.datetime.utcnow()
        time = start.strftime('%s')
        date = start.strftime('%Y%m%d')
        subs = 'agi technology worldnews machinelearning tensorflow datascience futurology bitcoin btc'.split()[0:5]
        branchNewModel = 'models/' + str(time)
        fb.put(branchNewModel, 'date', date)
        fb.put(branchNewModel, 'modelNum', len(getFbKeys('models')))
        branchReddit = branchNewModel + '/reddit'
        totalWords = 0
        for s in subs:
            branchSub = branchReddit + '/' + s
            branchPostIds = branchSub + '/postIds'
            posts = r.subreddit(s).hot(limit=50)
            wordCounts = {}
            totalSubWords = 0
            for p in posts:
                fb.put(branchPostIds, p.id, 1)
                words = textToWords(p.title)
                totalSubWords += len(words)
                wordCounts = getWordCount(words, wordCounts)
            fb.put(branchSub, 'totalWords', totalSubWords)
            totalWords += totalSubWords
            wc = filterWordCount(wordCounts)
            fb.put(branchSub, 'wordCounts', wc)
            fb.put(branchReddit, 'totalWords', totalWords)
        fb.put(branchNewModel, 'btc_price_est', 5000)
        # for post in r.subreddit(sr).hot(limit=1000):
        #     text += post.title
        # wordCount = getWordCount(words)
        # wordCountSorted = sorted(wordCount.items(), key=operator.itemgetter(1), reverse=True)
        # log.append(wordCountSorted[0:40])
            

        # get data

        # write records

        # review
        end = datetime.datetime.utcnow()
        elapsed = (end - start).seconds
        fb.put(branchNewModel, 'processing_time', elapsed)
        log.append('Lambda seconds: ' + str(elapsed))
        log.append('New models count: ' + str(len(getFbKeys('models'))))

    def test():
        pass
        return False

    test()
    main()
    return log

if __name__ == '__main__':
    import os
    os.system('clear')
    print 'Building and deploying package to Lambda'
    if config['fresh_build']:
        os.system('ls | grep -v lambda_function.py | xargs rm -r')
    if not os.path.exists('praw'):
        os.system('pip install praw -t .')
    if not os.path.exists('requests'):
      os.system('pip install requests -t .')
    if not os.path.exists('firebase'):
      os.system('pip install python-firebase -t .')
    # if not os.path.exists('pymysql'):
    #     os.system('pip install pymysql -t .')
    # if not os.path.exists('pymysql'):
    #   os.system('pip install pymysql -t .')
    os.system('zip -rq deployment-package.zip ./*')
    os.system('aws lambda update-function-code --function-name ' + config['function_name'] + ' --zip-file fileb://./deployment-package.zip')
    if config['clear_build']:
        os.system('ls | grep -v lambda_function.py | xargs rm -r')
    print 'Running function'
    os.system('aws lambda invoke --function-name ' + config['function_name'] + ' lambda_return.txt')
    os.system('clear')
    print 'Lambda return:'
    os.system('cat lambda_return.txt')
    os.system('rm lambda_return.txt')
    print
