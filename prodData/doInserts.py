import psycopg2
import pandas as pd
import math

conn = psycopg2.connect(host="127.0.0.1",
                        user="postgres",
                        password="postgres",
                        port="5432")
cursor = conn.cursor()


def create_dict_from_file(file_path):
    result_dict = {}
    
    with open(file_path, 'r') as file:
        lines = file.readlines()
        
        for line in lines[2:]:  # Skip the first two header lines
            parts = line.strip().split('|')
            if len(parts) == 2:
                mid = parts[0].strip()
                credit_link_id = parts[1].strip()
                result_dict[credit_link_id] = mid
    
    return result_dict

pairs = create_dict_from_file('pairs.txt')

df = pd.read_csv("reallyCleanGenres.csv", low_memory=False)

# for index, row in df.iterrows():
#     cli = str(row[0])
#     genre = row[1]
#     if cli in pairs:
#         # print("INSERT INTO genres (mid, genre) VALUES(%s, %s)", [pairs[cli], genre])
#         try:
#             cursor.execute("INSERT INTO genres (mid, genre) VALUES(%s, %s)", [pairs[cli], genre])
#         except:
#             continue

bad_vals = ['78802',
'161495',
'218473',
'287305',
'339428',
'10801',
'278978',
'117730',
'365371',
'215107',
'281085',
'282919',
'48144',
'145925',
'253632',
'108365',
'146341',
'331367',
'55146',
'43775',
'168283',
'60083',
'291634',
'293820',
'279698',
'154821',
'416437',
'77314',
'134368',
'296543',
'97805',
'279444',
'94214',
'207731',
'287991',
'109472',
'291675',
'78657',
'58886',
'277415',
'277724',
'279784',
'99885',
'29224',
'81346',
'61939',
'287816',
'266741',
'292974',
'288953',
'292090',
'266687',
'210937',
'65048',
'53688',
'175739',
'100594',
'32546',
'113178',
'193661',
'55396',
'68646',
'52920',
'155386',
'155796',
'22554',
'208385',
'200311',
'47499',
'253116',
'434306',
'55748',
'55500',
'61070',
'156908',
'210487',
'38869',
'155426',
'200796',
'64968',
'156917',
'91205',
'242737',
'60008',
'204996',
'66949',
'128856',
'154430',
'41427',
'140887',
'18120',
'63615',
'63764',
'272772',
'147683',
'148435',
'144089',
'147692',
'62775',
'281287',
'183946',
'120280',
'117499',
'174748',
'21245',
'60677',
'173865',
'30695',
'46341',
'423917',
'55403',
'74714',
'40970',
'90930',
'201646',
'370264',
'157178',
'72421',
'253074',
'71732',
'116751',
'65898',
'102527',
'148432',
'442417',
'333590',
'53364',
'81414',
'54354',
'367647',
'72375',
'4880',
'55408',
'145154',
'3962',
'142712',
'74183',
'449677',
'55763',
'55761',
'55754',
'148314',
'288108',
'289010',
'366044',
'55756',
'55759',
'36663',
'177697',
'193065',
'404479',
'55166',
'230680',
'92465',
'105287',
'132227',
'72419',
'434973',
'440178',
'86126',
'344741',
'62614',
'61121',
'74878',
'257608',
'162374',
'145594',
'31337',
'404471',
'79362',
'247500',
'98096',
'85883',
'399229',
'293097',
'251797',
'86727',
'61361',
'362151',
'78999',
'116019',
'458428',
'293094',
'118013',
'110491',
'79034',
'35623',
'112973',
'76703',
'370044',
'262243',
'288121',
'340543',
'292975',
'171961',
'288122',
'262988',
'288298',
'288117',
'288295',
'288284',
'82663',
'1997-08-20',
'288438',
'156180',
'62012',
'338853',
'288111',
'288146',
'217724',
'328712',
'288292',
'26152',
'128637',
'197752',
'288168',
'64362',
'397339',
'288290',
'288288',
'197723',
'288101',
'154792',
'177522',
'288109',
'72678',
'57307',
'172413',
'139909',
'128644',
'367678',
'76686',
'334317',
'60993',
'81225',
'134806',
'116059',
'62130',
'131822',
'142989',
'54356',
'412109',
'417399',
'149469',
'65646',
'55524',
'82662',
'38875',
'293552',
'388046',
'67272',
'83342',
'93649',
'60824',
'173914',
'205005',
'293198',
'65958',
'217038',
'219956',
'448538',
'455027',
'77776',
'161073',
'423078',
'19623',
'153196',
'82627',
'250503',
'52045',
'173433',
'239747',
'264113',
'309410',
'236661',
'176389',
'430985',
'85469',
'289336',
'222413',
'388182',
'130233',
'267219',
'110992',
'81600',
'91380',
'101352',
'297560',
'331958',
'52150',
'216369',
'271009',
'172890',
'52147',
'246320',
'158517',
'32547',
'219931',
'418029',
'142984',
'204765',
'263937',
'45514',
'68827',
'116894',
'159464',
'55825',
'394403',
'202183',
'391166',
'272543',
'173151',
'206574',
'60106',
'164419',
'44155',
'64934',
'288427',
'74950',
'273656',
'2692',
'76846',
'173300',
'329227',
'343693',
'358724',
'287831',
'182799',
'230295',
'42416',
'438012',
'292445',
'299203',
'273510',
'12076',
'127375',
'204768',
'117678',
'129966',
'198130',
'204965',
'151730',
'171357',
'308792',
'255940',
'124207',
'42438',
'56971',
'42441',
'38366',
'121510',
'48935',
'311021',
'215061',
'205349',
'285532',
'255906',
'52238',
'165159',
'127760',
'62725',
'212503',
'307113',
'129062',
'302118',
'55553',
'197335',
'82341',
'48805',
'307127',
'128946',
'261207',
'59040',
'56825',
'60014',
'56551',
'58189',
'58081',
'42674',
'43544',
'93855',
'255456',
'211710',
'251555',
'269165',
'3539',
'80472',
'211179',
'317247',
'287169',
'295689',
'241982',
'129359',
'30028',
'301883',
'58757',
'79892',
'113737',
'10639',
'145194',
'11719',
'168300',
'131503',
'76162',
'123283',
'84805',
'241170',
'58587',
'72917',
'76642',
'64357',
'157787',
'102534',
'30149',
'28573',
'257537',
'116649',
'226163',
'57342',
'62352',
'237171',
'275468',
'199219',
'63775',
'227383',
'74822',
'152635',
'183962',
'267654',
'287391',
'65718',
'131507',
'322260',
'294826',
'288313',
'117913',
'242240',
'246049',
'211561',
'146596',
'66584',
'121513',
'70989',
'50531',
'66700',
'195186',
'199985',
'157152',
'330275',
'142528',
'62243',
'105766',
'259616',
'257734',
'114438',
'333324',
'71191',
'15713',
'96872',
'62545',
'305310',
'284189',
'334651',
'107525',
'330588',
'289126',
'320299',
'335141',
'267623',
'86258',
'275244',
'75066',
'294517',
'246654',
'325382',
'332759',
'290043',
'122662',
'2012-09-29',
'252845',
'256328',
'62363',
'218772',
'337101',
'66082',
'156145',
'41703',
'240478',
'89330',
'119409',
'58611',
'58615',
'57918',
'173177',
'61303',
'75001',
'38285',
'121516',
'139563',
'2197',
'44486',
'161545',
'221171',
'61035',
'222517',
'87851',
'107052',
'59156',
'79750',
'56369',
'82481',
'186630',
'61911',
'195413',
'48937',
'283489',
'80374',
'56920',
'340961',
'54166',
'44937',
'297835',
'345896',
'13733',
'201913',
'58188',
'162382',
'62397',
'60046',
'60195',
'325780',
'60038',
'82350',
'213095',
'105509',
'273926',
'200117',
'246422',
'202980',
'82806',
'64155',
'58525',
'84056',
'285649',
'217412',
'213950',
'91527',
'288668',
'209248',
'213648',
'58487',
'143344',
'49711',
'347157',
'332354',
'131830',
'293270',
'297173',
'348507',
'57311',
'77000',
'63190',
'147868',
'133788',
'108664',
'46128',
'57713',
'77944',
'58518',
'58453',
'145872',
'160844',
'219067',
'166262',
'162282',
'58013',
'238705',
'171826',
'156547',
'270479',
'186982',
'182415',
'169069',
'249457',
'247218',
'231565',
'234356',
'228290',
'301272',
'262785',
'348544',
'38328',
'57112',
'58703',
'58714',
'62349',
'42675',
'58011',
'53957',
'35554',
'78854',
'105884',
'128625',
'348574',
'284362',
'267557',
'62713',
'44211',
'342052',
'108497',
'218688',
'318868',
'327685',
'262987',
'325645',
'120922',
'41669',
'41665',
'38315',
'106256',
'58070',
'311600',
'204771',
'238925',
'170455',
'284258',
'171873',
'353652',
'60071',
'124523',
'109170',
'288201',
'276155',
'261005',
'307293',
'8930',
'292387',
'64383',
'243059',
'231009',
'76871',
'316170',
'116236',
'284048',
'177104',
'281124',
'357130',
'324266',
'180759',
'252773',
'315335',
'68023',
'46436',
'63276',
'257237',
'300839',
'154671',
'243473',
'66447',
'295314',
'142982',
'292539',
'320028',
'47934',
'307276',
'362363',
'338312',
'322453',
'64526',
'60193',
'82368',
'92586',
'193732',
'273868',
'337014',
'80443',
'107035',
'268212',
'324017',
'260886',
'172396',
'160106',
'77241',
'52183',
'5881',
'121329',
'146970',
'310126',
'267389',
'263823',
'254725',
'252431',
'100783',
'337456',
'306391',
'306406',
'310458',
'312597',
'306411',
'302472',
'161442',
'42436',
'318562',
'305899',
'367006',
'325140',
'262113',
'365340',
'366763',
'298844',
'336033',
'116789',
'50189',
'74922',
'45990',
'60018',
'64948',
'61217',
'42579',
'327383',
'49907',
'67177',
'183964',
'62034',
'217648',
'53179',
'332534',
'319964',
'59349',
'84774',
'364684',
'207696',
'263873',
'72095',
'265351',
'30634',
'49828',
'257593',
'166207',
'306464',
'48845',
'295748',
'326874',
'285908',
'358980',
'279179',
'256375',
'152539',
'370524',
'277069',
'366548',
'338438',
'248706',
'163706',
'182843',
'162056',
'150958',
'299165',
'108228',
'131897',
'252028',
'55254',
'55776',
'62898',
'62896',
'62897',
'58074',
'56068',
'139230',
'315319',
'298722',
'272426',
'82745',
'366566',
'315534',
'249260',
'2014-01-01',
'153717',
'68360',
'68444',
'299856',
'73628',
'374698',
'182545',
'54832',
'284250',
'376358',
'277934',
'105991',
'360626',
'150049',
'135307',
'372113',
'313985',
'360627',
'74921',
'58553',
'329550',
'282768',
'48431',
'84035',
'147360',
'359807',
'53301',
'59264',
'309968',
'46442',
'222619',
'264036',
'222030',
'187737',
'334146',
'235450',
'365065',
'171902',
'270397',
'380841',
'380864',
'333888',
'335031',
'61314',
'78340',
'84865',
'359154',
'82157',
'43292',
'128866',
'370168',
'27941',
'382436',
'367492',
'54769',
'225244',
'203547',
'202172',
'331354',
'234815',
'82265',
'166256',
'74919',
'9939',
'379873',
'202020',
'156529',
'228184',
'258585',
'276387',
'220500',
'245324',
'131398',
'92988',
'66541',
'152653',
'185153',
'312399',
'282553',
'218728',
'49941',
'89560',
'79048',
'353641',
'64504',
'316111',
'280045',
'41078',
'104172',
'319179',
'329216',
'381670',
'71630',
'77958',
'342163',
'65216',
'44517',
'96497',
'187579',
'199879',
'96594',
'308671',
'284620',
'238456',
'267035',
'274202',
'79836',
'73835',
'354023',
'308823',
'2764',
'286512',
'326305',
'296225',
'71496',
'53472',
'347465',
'220154',
'119816',
'324408',
'180240',
'70119',
'85690',
'51839',
'164288',
'285935',
'24199',
'295782',
'252406',
'150736',
'153541',
'368128',
'19812',
'56344',
'254661',
'147496',
'297745',
'274110',
'354021',
'211158',
'64437',
'142406',
'262713',
'371758',
'69610',
'187238',
'383809',
'135536',
'138273',
'367596',
'297207',
'367275',
'66344',
'162611',
'140554',
'64407',
'174187',
'101858',
'54566',
'55495',
'171550',
'382962',
'49834',
'47683',
'341745',
'276157',
'76746',
'271664',
'304869',
'302802',
'109261',
'74705',
'177714',
'206099',
'355925',
'254439',
'191427',
'87992',
'75537',
'297063',
'279090',
'102444',
'343042',
'220515',
'384254',
'384237',
'93734',
'382885',
'306555',
'265717',
'264454',
'282266',
'14263',
'173587',
'66187',
'59126',
'120268',
'342850',
'231540',
'388055',
'119820',
'323573',
'308557',
'124013',
'288694',
'155011',
'112931',
'58007',
'227552',
'38061',
'381505',
'269306',
'51334',
'205557',
'112767',
'130638',
'63398',
'278935',
'202984',
'274266',
'367053',
'259634',
'46572',
'371942',
'359152',
'156954',
'342213',
'126043',
'63989',
'293516',
'316003',
'237799',
'112675',
'126140',
'138061',
'183073',
'47535',
'208173',
'338079',
'340119',
'378607',
'260883',
'256916',
'144421',
'267497',
'267506',
'411711',
'262786',
'407048',
'279842',
'246829',
'124202',
'73755',
'38289',
'105734',
'410576',
'407965',
'59858',
'270251',
'320037',
'372640',
'413430',
'414173',
'414382',
'414632',
'244971',
'416124',
'162899',
'375167',
'347666',
'413806',
'399621',
'132608',
'73160',
'321779',
'410455',
'175791',
'333884',
'53805',
'378087',
'359483',
'343227',
'418979',
'322455',
'352917',
'402612',
'410142',
'271033',
'34092',
'318184',
'42100',
'420703',
'164134',
'421958',
'315252',
'258113',
'405763',
'263260',
'366887',
'278544',
'393407',
'203539',
'226001',
'266568',
'410774',
'216647',
'51959',
'313896',
'374319',
'279558',
'268712',
'374247',
'73077',
'419289',
'41619',
'64965',
'333516',
'148479',
'297298',
'371153',
'140485',
'223551',
'80961',
'56486',
'320293',
'59046',
'365544',
'290911',
'216550',
'361340',
'294608',
'416258',
'53693',
'292538',
'430514',
'318794',
'189807',
'190605',
'193793',
'390547',
'356461',
'392832',
'424014',
'375872',
'327083',
'380771',
'254299',
'82716',
'4652',
'83271',
'226675',
'428645',
'409082',
'43989',
'295011',
'369603',
'297342',
'376188',
'170548',
'72822',
'437253',
'47241',
'420519',
'394723',
'437838',
'384373',
'58928',
'357155',
'265432',
'83729',
'177979',
'407430',
'179366',
'358293',
'320318',
'186592',
'440361',
'443113',
'306218',
'439263',
'442752',
'56947',
'224233',
'278475',
'408550',
'382474',
'346758',
'419522',
'366561',
'447511',
'298920',
'342011',
'391438',
'416569',
'413687',
'128892',
'336956',
'109861',
'448290',
'362617',
'227964',
'342684',
'359413',
'77564',
'327909',
'375298',
'449863',
'142478',
'41663',
'185180',
'453062',
'428950',
'440508',
'152100',
'167330',
'426224',
'220669',
'364123',
'240992',
'38547',
'366759',
'148697',
'49833',
'48763',
'452606',
'65010',
'101217',
'236053',
'123592',
'109671',
'69319',
'74171',
'327935',
'123601',
'123611',
'409926',
'453596',
'455363',
'455368',
'142802',
'441483',
'439998',
'77534',
'143883',
'354133',
'191486',
'127803',
'401150',
'271495',
'348858',
'244575',
'246438',
'400796',
'362844',
'36264',
'270908',
'14210',
'441826',
'376934',
'213321',
'381054',
'380438',
'41493',
'452922',
'93461',
'340155',
'63838',
'197057',
'143005',
'336484',
'186766',
'159810',
'51275',
'420481',
'69976',
'26792',
'37603',
'48209',
'456956',
'57382',
'110131',
'41689',
'458808',
'400552',
'449012',
'419601',
'14644',
'82495',
'64827',
'333953',
'103301',
'301876',
'73545',
'448879',
'457307',
'396987',
'461089',
'236017',
'292033',
'461533',
'153561',
'447169',
'195645',
'195542',
'195612',
'195592',
'237156',
'237139',
'232137',
'196491',
'174565',
'366860',
'202865',
'9765',
'213683',
'57770',
'425916',
'456018',
'142320',
'430058',
'54309',
'384669',
'445840',
'64043',
'73649',
'57996',
'63179',
'398295',
'353713',
'298695',
'405576',
'464819',
'458335',
'448992',
'298207',
'382995',
'439314',
'422005',
'26969',
'91673',
'438910',
'68063',
'433711',
'103344',
'275272',
'231216',
'467731',
'79343',
'418757',
'369444',
'245394',
'212865',
'335251',
'395767',
'199887',
'317389',
'468707',
'280422',
'449131',
'257472',
'439050']

df = pd.read_csv("reallyCleanCredits.csv", low_memory=False)
for index, row in df.iterrows():
    cli = str(row[0])
    name = row[1]
    role = row[2]
    character = row[3]
    # print(role == 'crew')
    # print(type(cli))
    # print(cli == '78802')
    if(role != 'crew' and cli not in bad_vals):
        if (type(character) == str and not len(character) >= 255) or type(character) == float:
        # print(pairs[cli])
            try:
                if(type(character) == float):
                    formatted = f"INSERT INTO movie_cast (mid, name, role, character) VALUES({pairs[cli]}, '{name}', '{role}', NULL);"
                else:
                    formatted = f"INSERT INTO movie_cast (mid, name, role, character) VALUES({pairs[cli]}, '{name}', '{role}', '{character}');"
                cursor.execute(formatted)
            except Exception as e: 
                print("error",e)
                print(formatted)
                # continue
                quit()

