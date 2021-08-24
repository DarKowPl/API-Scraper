import random

import orm
from parsers import DataParser, UrlRequest, RequestParameters
from threading import Event
from datetime import datetime

pages_range = []
main_advertise_urls_with_settings = {}
single_adverts_links = []

request_parameters = RequestParameters()


def main():
    urls = []

    for page in UrlRequest().get_content(request_parameters.set_start_activity_settings_for_requests()):

        if page.url == request_parameters.get_main_page_url()[0]:
            for _ in range(len(request_parameters.proxies)):
                page_urls = DataParser(page.content).get_start_activity_urls_from_main_page()
                urls.extend(request_parameters.build_start_urls_list(page_urls))

        elif page.url == request_parameters.get_main_category_endpoint()[0]:
            last_page_number = DataParser(page.content).get_last_page_number()
            pages_range.extend(request_parameters.build_page_range_list(int(last_page_number)))
            mixed_advertises: list = request_parameters.mix_advertises_pages(pages_range)

            for i in range(len(request_parameters.proxies)):
                urls[i].extend(mixed_advertises[i])

            main_advertise_urls_with_settings.update(request_parameters.set_settings_for_main_advertise_list(urls))

            with open('links', 'a+') as file_1:
                file_1.write('Start\n')
                file_1.write(str(datetime.now())[:-7].replace('-', '_').replace(' ', '_'))
                for i in main_advertise_urls_with_settings:
                    for urls in main_advertise_urls_with_settings[i]['urls']:
                        file_1.write(urls + '\n')
                    file_1.write('Stop\n' * 5)


def main_2():
    main_while_condition = True
    while main_while_condition:
        for dict_key in main_advertise_urls_with_settings:
            main_page_request = UrlRequest().get_content_2(main_advertise_urls_with_settings[dict_key], dict_key)
            main_page_request = next(main_page_request)
            Event().wait(3)
            with open('main_pages_information', 'a+') as file:
                file.write('Start\n')
                file.writelines(str(main_page_request.url) + '\n')
                file.writelines(str(main_page_request.headers) + '\n')
                file.writelines(str(main_page_request.request.headers) + '\n')
                file.write('Stop\t' * 5 + '\n')

            single_adverts_links.extend(
                DataParser(main_page_request.content).get_all_advertisements_links_from_main_pages(
                    request_parameters.get_skippable_urls(),
                    main_page_request.url
                )
            )

            if len(single_adverts_links) != 0:
                updated_single_adverts_links = request_parameters.copy_settings_from_main_adverts_list(
                    dict_key,
                    single_adverts_links.copy()
                )

                second_set_urls = request_parameters.add_all_single_adverts_links(dict_key, updated_single_adverts_links)
                # if dict_key in second_set_urls:
                #     for i in updated_single_adverts_links.get(dict_key).get('urls'):
                #         second_set_urls[dict_key]['urls'].append(i)
                #
                # while dict_key not in second_set_urls:
                #     second_set_urls.update(updated_single_adverts_links)

                single_adverts_links.clear()

                condition = True
                while condition:

                    page_2 = UrlRequest().get_content_2(second_set_urls[dict_key], dict_key)
                    page_2 = next(page_2)
                    Event().wait(3)

                    content = DataParser(page_2.content)

                    content.get_category_of_advertisement()
                    content.get_advert_title()
                    content.get_advert_link(page_2.url)
                    content.get_advert_stats()
                    content.get_advert_description()

                    advert_details: dict = content.get_core_details()
                    advert_details.update(content.get_advert_stats())
                    advert_details['Date'] = datetime.now().isoformat(' ', 'seconds')

                    add_advert = orm.TrojScrapperBase(**advert_details)
                    orm.session.add(add_advert)
                    orm.session.commit()
                    print(advert_details)
                    print('*' * 80)

                    with open('core_deatails', 'a+') as file:
                        file.write(str(datetime.now())[:-7].replace('-', '_').replace(' ', '_') + '\n')
                        file.writelines(str(advert_details) + '\n')
                        file.write('*' * 30 + '\n')
                    import wdb;
                    wdb.set_trace()
                    if second_set_urls[dict_key]['urls']:
                        second_set_urls[dict_key]['urls'].pop(0)

                        if len(second_set_urls) == len(request_parameters.proxies):
                            dict_key = [k for k, v in second_set_urls.items() if v.get('urls')]

                            if dict_key is not []:
                                dict_key = random.choice(dict_key)

                            else:
                                condition = False
                    else:
                        condition = False

                    if len(second_set_urls) != len(request_parameters.proxies):
                        break

            else:
                if main_advertise_urls_with_settings[dict_key]['urls']:
                    main_advertise_urls_with_settings[dict_key]['urls'].pop(0)

                    if main_advertise_urls_with_settings[dict_key]['urls'] is []:
                        main_while_condition = False


#   exit while loop condition put here

if __name__ == '__main__':
    main()
    main_2()
# import wdb; wdb.set_trace()
