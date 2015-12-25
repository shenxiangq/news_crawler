import logging
from tld import get_tld

def init_log(filename):
    logging.basicConfig(filename)

def valid_a_href(a_elements, main_url=None):
    hrefs = [a.get('href') for a in a_elements]
    hrefs = [link for link in hrefs if link and link.startswith('http://')]
    if main_url:
        main_tld = get_tld(main_url, fail_silently=True)
        hrefs = [link for link in hrefs if get_tld(link, fail_silently=True) == main_tld]

    return hrefs
