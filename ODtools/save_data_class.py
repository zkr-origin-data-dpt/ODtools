# -*- coding: utf-8 -*-
# @Author   : FJ
import json
import time
import hashlib
import pymysql


class SaveOriginalData(object):
    def __init__(self, username: str, password: str, ip: str, host: int=3306, db: str="fangjie"):
        '''
        :param username: mysql username
        :param password: mysql password
        :param ip: mysql ip
        :param host: mysql host
        '''
        self.data_source = {
                "weibo_info": "original_data_weibo_info",
                "weibo_user": "original_data_weibo_user",
                "news_info": "original_data_news_info",
                "wechat_info": "original_data_wechat_info",
                "wechat_user": "original_data_wechat_user",
            }
        self.username = username
        self.password = password
        self.ip = ip
        self.host = host
        self.db = db
        for i in range(3):
            try:
                self.db = pymysql.connect(host=self.ip, port=self.host, user=self.username, password=self.password, db=self.db)
                self.cursor = self.db.cursor()
                print("Instantiation SQLClient success")
                break
            except:
                continue
        else:
            raise

    def get_rowkey(self, data):
        return data["rowkey"]

    def save(self, original_data, data_type):
        sql = "insert into {} (id,url,data,data_update_time,complation) values(%s,%s,%s,%s,'0')".format(self.data_source[data_type])
        data_update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        try:
            # 传来的是json
            if type(original_data) == str:
                data = json.loads(original_data)
                url = self.get_rowkey(data)
                id = hashlib.md5(url.encode()).hexdigest()
                self.cursor.execute(sql, (id, url, json.dumps(data), data_update_time))
            # 传来的是字典
            elif type(original_data) == dict:
                data = original_data
                url = self.get_rowkey(data)
                id = hashlib.md5(url.encode()).hexdigest()
                self.cursor.execute(sql, (id, url, json.dumps(data), data_update_time))
            # 传来的是列表或集合或元组
            else:
                for one_data in original_data:
                    if type(one_data) == str:
                        data = json.loads(one_data)
                        url = self.get_rowkey(data)
                        id = hashlib.md5(url.encode()).hexdigest()
                        self.cursor.execute(sql, (id, url, json.dumps(data), data_update_time))
                    elif type(one_data) == dict:
                        data = one_data
                        url = self.get_rowkey(data)
                        id = hashlib.md5(url.encode()).hexdigest()
                        self.cursor.execute(sql, (id, url, json.dumps(data), data_update_time))
            self.db.commit()
            print("原始数据入库成功")
            return True
        except Exception as e:
            self.db.rollback()
            import traceback
            traceback.print_exc()
            print("存储出错, 错误原因:{}".format(e))

    def __del__(self):
        self.cursor.close()
        self.db.close()
        print("实例化释放")

if __name__ == '__main__':
    # from ODtools import HBaseClient
    # # 测试
    # hb = HBaseClient("192.168.129.231", 9090)
    # table_name = "NEWS_INFO_TABLE"
    # result = hb.scan_result(table_name)
    # sql_clict = SaveOriginalData("zkr_cj", "zkrcj123", "192.168.129.224", 3306, db="fangjie")
    # num = 1
    # for i in result:
    #     if i.get("m_project_name", "") == "yqsj15":
    #     # if i.get("m_project_name", "") == "cbfx62":
    #         print(i)
    #         print(num)
    #         sql_clict.save(i, "news_info")
    #         num += 1
    #         if num >= 100:
    #             break

    item = {'rowkey': 'http://www.chinatimes.net.cn/article/48329.html', 'm_content_url': 'http://www.chinatimes.net.cn/article/48329.html', 'm_origin_url': 'http://www.chinatimes.net.cn', 'm_domain_name': '华夏时报网', 'r_comment_num': 0, 'r_praised_num': 0, 'r_respond_num': '', 'r_read_num': 0, 'm_catalog_name': ['首页', '财讯', '正文'], 'm_editor': '作者：老沉', 'm_content': '中秋，于古人，是在时令交错转换间的一场祭祀；于国人，是遥寄乡土思恋的一阙诗卷。一方家宴，一轮明月，圈起展席而坐的亲情挚爱，在一年收获的时令相聚拥趸，家族、家庭、回归，无论在何地，中秋都是中国人对“家”的终极追溯。以酬佳节“中秋”，上古便承载着先民对月的崇拜，每逢秋季的第二个月被称为“仲秋”的日子，人们总是惊诧于挂于天际的那轮浩大的银盘会在空中高悬。于是，被记录在《周礼》一书中作为历法的时间，同时，月圆之夜的朗星之下，人们阖家携老在祠堂前巨大的香案上摆放时令果品，迎寒和祭月的习俗由此而生，绵延千年。对月的崇拜和神话，更多地给文人提供了最广阔的创作素材。“此夜若无月，一年虚度秋。”唐朝，中秋的月成了不可或缺的存在，成了一年中最深刻的盼望。乃至政府专门颁布法令为中秋特设三天假期，中秋，终于以节日的身份正式进入中国人的年历表。上至皇亲贵胄下至黎民百姓全天纵情饮酒，拜月的习俗也从皇家的祭祀典礼变成了小儿女对月老不可言说的祈愿。“万里无云镜九州，最团圆夜是中秋”，中秋明月象征团圆的美好意向植根在中国人的骨血里，“安排家宴，团圆子女，以酬佳节”成为不变的定式。徽派家宴中秋的前夜，我驱车前往丰乐湖。这片被誉为黄山峰外第一峰的所在，距离黄山的主景区仅38公里。早在此前的几日，便把家中老人和妻儿托付给德懋堂的“管家”，此刻他们已经在丰乐湖畔提前享受了好几日的定制假期。一片叠翠掩映之间，白墙灰瓦，错落着三座百年的徽州古宅。德懋堂，取“顺应自然，勤勉茂盛”之意，原来的匾额悬挂在正厅当中，宅子还是原来的宅子，但原建的基石却距此地甚远。据说起初这三座浩大的徽州古庭院，都原建在徽州的偏僻村落，长年累月风化失修，古宅的后人却无力修缮，如同看着曾经华丽的女子在时光的风雨中飘摇殆尽，却无力回天。直到被“德懋堂”的老板看中，才被一一编号从原来的僻壤移居至黄山脚下，逐一修复，按照顺序古法搭建。周围林立了逾百栋徽派养生的度假别墅。德懋堂，既成了别墅群落的本尊建筑，也是最具徽派传承的食肆。第二日的清晨，让“管家”定制了到呈坎的行程。和丰乐湖同为黄山南麓，呈坎古村落有着1800多年的历史和传承不变的“游呈坎一生无坎”的传奇过坎文化。除了典型的徽派白墙灰瓦的建筑，呈坎更像是圣地麦加。对于易经八卦之学的深刻领悟，对地理学、环境学的完美实践，对徽派古典艺术的高度弘扬，使呈坎成为一种标尺。天下熙熙，皆为呈坎而来；天下攘攘，皆为呈坎而往。我们去时正值中秋的祈愿之日，使呈坎吸引了更多虔诚的目光和脚步。丰乐湖水质清澈，下午两三点，德懋堂渔业部的员工便会行船至丰乐湖中心，那里放养着不计其数的花白鲢鱼，特制的渔网只会捕捞七斤以上的鲢鱼，这种捕捞日日不懈。带着家人用另一条小舟随渔船出航，在距离捕鱼船最近的地方，我挑选了一条中意的鲢鱼，足有七八斤的样子。据说，这些被人工放养近于野生的鲢鱼，最大的几乎可以齐成人腰线以上。在“无鱼不成宴”的中式宴席中，硕大的鲢鱼头，配以当地的豆腐以土法炖煮良久，自成一道极具本土风味的主菜。平日忙于工作，这条寓意有余的大菜，算是我对家人的一点补偿。月华初上，家宴的高潮是“德懋堂”提前准备的月饼制作食材，从面饼到锞模乃至各色饼馅一应俱全，全家齐齐动手，更成了孩子们最喜欢的一个程序。徽州月饼不同别地的味道和烤制方法，类似苏式月饼的皮层酥松、内馅饱满，却是采用煎制的工艺，让今年的月饼显得与众不同。“月亮霞光照九州,花香四溢沁人心。一家老少堂前聚,美酒茅台饮数斤（《中秋团圆夜》）。”迎着月色，全家泛舟丰乐湖，和白日的风景不同，水波如镜倒映出的一轮明月，附近隐约是烟熏样的水墨青峰。白墙灰瓦的徽州古宅，用它的娴静与清新，和对哲学与审美的完美诠释，传递着徽州人对家族延续的重视与培养。在德懋堂附近，即林立着鲍氏一门十八代人在明清两朝、四百多年间矗立的七座牌坊，汇聚着家族、宗祠和他们的荣耀。驱车几十分钟到附近的老街，沿着明月当头的青石板路前行，遇到走街串巷的货郎，一头挑干柴，一头挑毛豆腐，浇上香油，淋上辣椒糊，就着油锅边吃边聊，恍然如时光倒流，穿越从头。海宁追潮阔别徽州的静逸，驱车290公里，今天是一年一度追潮的日子。“大海之水，朝生为潮，夕生为汐（余道安《海潮图序》）。”月圆之日，正是太阳与月亮产生合力之时，潮水大涨。中秋时节的日月合力达到全年最盛，海潮也为全年之最。潮头的推进滞后于它的原生，当那巨浪拍打到岸边时，已是三天之后，农历八月十八日。路上已经有不少车辆和我们奔赴同一个目的地。“江干上下十余里间，珠翠罗绮溢目，车马塞途，饮食百物皆倍穹常时，而僦赁看幕，虽席地不容间也（南宋周密《武林旧事》）。”遥想当年为了观潮，全家携老带幼席地而坐，车马拥堵在江畔的景象，对于今天仍把追潮作为一年乐事的现代人，我转换的只是交通的工具，而心境仍免不了一番惺惺相惜后的慨叹。钱塘观潮的习俗，最初源于宋朝法令，法令规定每年农历八月十八日，于钱塘江上校阅水军。“每岁京尹出浙江亭教阅水军，艨艟数百，分列两岸，既而尽奔腾分合五阵之势，并有乘骑弄旗标枪舞刀于水面者，如履平地（周密《武林旧事》）。”整个校阅过程对民间开放，可以自由参观，除此之外，还有一个精彩的弄潮环节，“吴儿善泅者数百，皆披发文身，手持十幅大彩旗，争先鼓勇，溯迎而上，出没于鲸波万仞中，腾身百变，而旗尾略不沾湿，以此夸能。”高超的技艺引起百姓争相观看。虽然弄潮活动因为过于危险，每年死伤惨重而被政府明令取缔，但观潮的习俗却在民间被保留下来。徐志摩的家乡浙江海宁，正位于那河道最狭窄处，自古成为观潮胜地。海宁盐官的“一线横江”的壮阔景象更是被称为天下一绝。然而钱塘江潮的精彩壮观，决不仅止那一线，能够看到海潮咆哮的观潮点也决不止盐官一处。今人比古人幸运的是可以驾车追潮。海宁的盐官古镇距离杭州五十公里，如果不开车或者假期有限，倒不妨在杭州本地以及海宁市区租车。驾驶一辆七座全封闭式的家庭座驾，随着潮头以每小时35公里的速度追潮而行，即可饱览钱塘潮的全部壮阔。我们的车行至盐官新仓丁桥镇大缺口处，此时平静浩荡的江面下暗藏玄机——江心的巨大沙洲随时准备将西进的海潮一劈为二。两股潮头在经过沙洲后，虽继续西进，但角度已变，二潮交叉前行，形成壮观的交叉潮。分道两股的潮水行至盐官，居然心有灵犀完全合拢，横扫江面，形成一线潮。潮未至，震耳欲聋的潮水声却早已提前昭告世人自己的大驾光临。即使坐在车内也能感受到这种震天撼地的强悍。谈到观潮，其实决不仅止于一个“观”字，更要用双耳聆听潮水的咆哮，用全身上下所有的毛孔体会潮头涌过水面带来的震撼。潮水西进之后将在老盐仓进入另一个高潮。潮头将被河道中六百多米长的拦河丁坝挡住去路，咆哮折返后，与前进的潮头相撞，形成壮观的回头潮。早在唐朝就已经有古人推算出精准的《四时潮候图》，是钱塘观潮的绝佳攻略。千百年之后，河道、气候虽已改变，曾经的推算方法依旧成立，只是潮头经过的时间统统滞后了近一个小时。追潮之前，我专门从网上下载了这古今双份的观潮攻略，两相参照，倒也可增添不少趣味。本文版权归CM华夏理财所有，未经许可不得翻译或转载。查看更多华夏时报文章，参与华夏时报微信互动（微信搜索「华夏时报」或「chinatimes」）', 'm_title': '追月：黄山家宴 海宁潮', 'm_images': ['http://styles.chinatimes.net.cn/images/hxsb_logo.png?v=1587690894', 'http://uploads.chinatimes.net.cn/article/201504/BIG201309131416966.png', 'http://styles.chinatimes.net.cn/images/weixin.jpg', 'http://uploads.chinatimes.net.cn/ad/201605/20160524174311N0CDBczBY2.png', 'http://uploads.chinatimes.net.cn/user/avatar.jpg', 'http://styles.chinatimes.net.cn/images/nopic.jpg?v=1587690894', 'http://styles.chinatimes.net.cn/images/nopic.jpg?v=1587690894', 'http://styles.chinatimes.net.cn/images/nopic.jpg?v=1587690894', 'http://uploads.chinatimes.net.cn/article/202004/200_20200415121900Yx5cqhLGVY.jpg', 'http://uploads.chinatimes.net.cn/article/202004/200_20200415124019fnxuPM9dm0.jpg', 'http://uploads.chinatimes.net.cn/article/202004/200_20200415111455qljeN0KkR8.jpg', 'http://uploads.chinatimes.net.cn/article/202004/200_20200411194328qB0ZuUNSgl.jpg', 'http://uploads.chinatimes.net.cn/article/202004/200_20200410203040GADY9znDpR.png', 'http://uploads.chinatimes.net.cn/article/202004/200_20200410174843YmyJQ6qwGs.jpg', 'http://uploads.chinatimes.net.cn/article/202004/200_20200411144840UMSwpePKCb.png', 'http://uploads.chinatimes.net.cn/article/202004/200_20200417102254P1UkwQG0Wm.jpg', 'http://uploads.chinatimes.net.cn/article/202004/200_20200410205639ZCCZFMRQaW.png', 'http://uploads.chinatimes.net.cn/user/201702/80_201702091559142c2Sq0QDx2.jpg', 'http://uploads.chinatimes.net.cn/user/201910/80_20191031123440Z0EDuptJeq.jpg', 'http://uploads.chinatimes.net.cn/user/201508/80_201508311437155h6sYGRUJh.jpg', 'http://uploads.chinatimes.net.cn/user/201508/80_20150831150020wBWea2yzFN.jpg', 'http://uploads.chinatimes.net.cn/user/201508/80_201508311603202ef7SM9nRF.jpg', 'http://uploads.chinatimes.net.cn/user/201901/80_20190125153111A3AMcYdkKs.jpg', 'http://uploads.chinatimes.net.cn/user/201803/80_20180308190055Nf72jGgvDl.jpg', 'http://uploads.chinatimes.net.cn/user/201704/80_20170404211414geREZ4f106.jpg', 'http://uploads.chinatimes.net.cn/user/201702/80_20170224192310AxJTAuWcte.jpg', 'http://styles.chinatimes.net.cn/images/hxsb_epaper.jpg?at=1', 'http://styles.chinatimes.net.cn/images/gonganbeian.png', 'http://styles.chinatimes.net.cn/images/xinlangweibo.png?v=1587690894', 'http://styles.chinatimes.net.cn/images/weixin.jpg?v=1587690894', 'http://styles.chinatimes.net.cn/images/shuipiweixin.png?v=1587690894'], 'm_videos': '', 'g_publish_time': '2013-09-16 17:03:00', 'g_spider_time': '2020-04-24 09:17:11', 'm_domain': 'chinatimes.net.cn', 'm_relation': '来源：华夏时报', 'm_audios': '', 'g_update_time': '2020-04-24 09:17:11', 'm_history_effectiveness': '1', 'm_source': ['news', 'news_history'], 'm_crawl_origin': {'origin_type': 'news_history', 'value': '', 'level': 1, 'domain_url': 'http://www.chinatimes.net.cn/user/3110.html'}, 'm_project_name': 'yqsj15'}
    sql_clict = SaveOriginalData("zkr_cj", "zkrcj123", "192.168.129.224", 3306, db="fangjie")
    sql_clict.save(item, "news_info")
