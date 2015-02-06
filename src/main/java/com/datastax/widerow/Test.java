package com.datastax.widerow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;

public class Test {

	private static final int NO_OR_ROWS = 50000;
	private Logger logger = LoggerFactory.getLogger(Test.class);
	private Session session;

	private String insert = "insert into test.bigrow(key,col0,col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29,col30,col31,col32,col33,col34,col35,col36,col37,col38,col39,col40,col41,col42,col43,col44,col45,col46,col47,col48,col49,col50,col51,col52,col53,col54,col55,col56,col57,col58,col59,col60,col61,col62,col63,col64,col65,col66,col67,col68,col69,col70,col71,col72,col73,col74,col75,col76,col77,col78,col79,col80,col81,col82,col83,col84,col85,col86,col87,col88,col89,col90,col91,col92,col93,col94,col95,col96,col97,col98,col99,col100,col101,col102,col103,col104,col105,col106,col107,col108,col109,col110,col111,col112,col113,col114,col115,col116,col117,col118,col119,col120,col121,col122,col123,col124,col125,col126,col127,col128,col129,col130,col131,col132,col133,col134,col135,col136,col137,col138,col139,col140,col141,col142,col143,col144,col145,col146,col147,col148,col149,col150,col151,col152,col153,col154,col155,col156,col157,col158,col159,col160,col161,col162,col163,col164,col165,col166,col167,col168,col169,col170,col171,col172,col173,col174,col175,col176,col177,col178,col179,col180,col181,col182,col183,col184,col185,col186,col187,col188,col189,col190,col191,col192,col193,col194,col195,col196,col197,col198,col199,col200,col201,col202,col203,col204,col205,col206,col207,col208,col209,col210,col211,col212,col213,col214,col215,col216,col217,col218,col219,col220,col221,col222,col223,col224,col225,col226,col227,col228,col229,col230,col231,col232,col233,col234,col235,col236,col237,col238,col239,col240,col241,col242,col243,col244,col245,col246,col247,col248,col249,col250,col251,col252,col253,col254,col255,col256,col257,col258,col259,col260,col261,col262,col263,col264,col265,col266,col267,col268,col269,col270,col271,col272,col273,col274,col275,col276,col277,col278,col279,col280,col281,col282,col283,col284,col285,col286,col287,col288,col289,col290,col291,col292,col293,col294,col295,col296,col297,col298,col299,col300,col301,col302,col303,col304,col305,col306,col307,col308,col309,col310,col311,col312,col313,col314,col315,col316,col317,col318,col319,col320,col321,col322,col323,col324,col325,col326,col327,col328,col329,col330,col331,col332,col333,col334,col335,col336,col337,col338,col339,col340,col341,col342,col343,col344,col345,col346,col347,col348,col349,col350,col351,col352,col353,col354,col355,col356,col357,col358,col359,col360,col361,col362,col363,col364,col365,col366,col367,col368,col369,col370,col371,col372,col373,col374,col375,col376,col377,col378,col379,col380,col381,col382,col383,col384,col385,col386,col387,col388,col389,col390,col391,col392,col393,col394,col395,col396,col397,col398,col399,col400,col401,col402,col403,col404,col405,col406,col407,col408,col409,col410,col411,col412,col413,col414,col415,col416,col417,col418,col419,col420,col421,col422,col423,col424,col425,col426,col427,col428,col429,col430,col431,col432,col433,col434,col435,col436,col437,col438,col439,col440,col441,col442,col443,col444,col445,col446,col447,col448,col449,col450,col451,col452,col453,col454,col455,col456,col457,col458,col459,col460,col461,col462,col463,col464,col465,col466,col467,col468,col469,col470,col471,col472,col473,col474,col475,col476,col477,col478,col479,col480,col481,col482,col483,col484,col485,col486,col487,col488,col489,col490,col491,col492,col493,col494,col495,col496,col497,col498,col499,col500,col501,col502,col503,col504,col505,col506,col507,col508,col509,col510,col511,col512,col513,col514,col515,col516,col517,col518,col519,col520,col521,col522,col523,col524,col525,col526,col527,col528,col529,col530,col531,col532,col533,col534,col535,col536,col537,col538,col539,col540,col541,col542,col543,col544,col545,col546,col547,col548,col549,col550,col551,col552,col553,col554,col555,col556,col557,col558,col559,col560,col561,col562,col563,col564,col565,col566,col567,col568,col569,col570,col571,col572,col573,col574,col575,col576,col577,col578,col579,col580,col581,col582,col583,col584,col585,col586,col587,col588,col589,col590,col591,col592,col593,col594,col595,col596,col597,col598,col599,col600,col601,col602,col603,col604,col605,col606,col607,col608,col609,col610,col611,col612,col613,col614,col615,col616,col617,col618,col619,col620,col621,col622,col623,col624,col625,col626,col627,col628,col629,col630,col631,col632,col633,col634,col635,col636,col637,col638,col639,col640,col641,col642,col643,col644,col645,col646,col647,col648,col649,col650,col651,col652,col653,col654,col655,col656,col657,col658,col659,col660,col661,col662,col663,col664,col665,col666,col667,col668,col669,col670,col671,col672,col673,col674,col675,col676,col677,col678,col679,col680,col681,col682,col683,col684,col685,col686,col687,col688,col689,col690,col691,col692,col693,col694,col695,col696,col697,col698,col699) " + 
			" values " +  
			"(?,'val0','val1','val2','val3','val4','val5','val6','val7','val8','val9','val10','val11','val12','val13','val14','val15','val16','val17','val18','val19','val20','val21','val22','val23','val24','val25','val26','val27','val28','val29','val30','val31','val32','val33','val34','val35','val36','val37','val38','val39','val40','val41','val42','val43','val44','val45','val46','val47','val48','val49','val50','val51','val52','val53','val54','val55','val56','val57','val58','val59','val60','val61','val62','val63','val64','val65','val66','val67','val68','val69','val70','val71','val72','val73','val74','val75','val76','val77','val78','val79','val80','val81','val82','val83','val84','val85','val86','val87','val88','val89','val90','val91','val92','val93','val94','val95','val96','val97','val98','val99','val100','val101','val102','val103','val104','val105','val106','val107','val108','val109','val110','val111','val112','val113','val114','val115','val116','val117','val118','val119','val120','val121','val122','val123','val124','val125','val126','val127','val128','val129','val130','val131','val132','val133','val134','val135','val136','val137','val138','val139','val140','val141','val142','val143','val144','val145','val146','val147','val148','val149','val150','val151','val152','val153','val154','val155','val156','val157','val158','val159','val160','val161','val162','val163','val164','val165','val166','val167','val168','val169','val170','val171','val172','val173','val174','val175','val176','val177','val178','val179','val180','val181','val182','val183','val184','val185','val186','val187','val188','val189','val190','val191','val192','val193','val194','val195','val196','val197','val198','val199','val200','val201','val202','val203','val204','val205','val206','val207','val208','val209','val210','val211','val212','val213','val214','val215','val216','val217','val218','val219','val220','val221','val222','val223','val224','val225','val226','val227','val228','val229','val230','val231','val232','val233','val234','val235','val236','val237','val238','val239','val240','val241','val242','val243','val244','val245','val246','val247','val248','val249','val250','val251','val252','val253','val254','val255','val256','val257','val258','val259','val260','val261','val262','val263','val264','val265','val266','val267','val268','val269','val270','val271','val272','val273','val274','val275','val276','val277','val278','val279','val280','val281','val282','val283','val284','val285','val286','val287','val288','val289','val290','val291','val292','val293','val294','val295','val296','val297','val298','val299','val300','val301','val302','val303','val304','val305','val306','val307','val308','val309','val310','val311','val312','val313','val314','val315','val316','val317','val318','val319','val320','val321','val322','val323','val324','val325','val326','val327','val328','val329','val330','val331','val332','val333','val334','val335','val336','val337','val338','val339','val340','val341','val342','val343','val344','val345','val346','val347','val348','val349','val350','val351','val352','val353','val354','val355','val356','val357','val358','val359','val360','val361','val362','val363','val364','val365','val366','val367','val368','val369','val370','val371','val372','val373','val374','val375','val376','val377','val378','val379','val380','val381','val382','val383','val384','val385','val386','val387','val388','val389','val390','val391','val392','val393','val394','val395','val396','val397','val398','val399','val400','val401','val402','val403','val404','val405','val406','val407','val408','val409','val410','val411','val412','val413','val414','val415','val416','val417','val418','val419','val420','val421','val422','val423','val424','val425','val426','val427','val428','val429','val430','val431','val432','val433','val434','val435','val436','val437','val438','val439','val440','val441','val442','val443','val444','val445','val446','val447','val448','val449','val450','val451','val452','val453','val454','val455','val456','val457','val458','val459','val460','val461','val462','val463','val464','val465','val466','val467','val468','val469','val470','val471','val472','val473','val474','val475','val476','val477','val478','val479','val480','val481','val482','val483','val484','val485','val486','val487','val488','val489','val490','val491','val492','val493','val494','val495','val496','val497','val498','val499','val500','val501','val502','val503','val504','val505','val506','val507','val508','val509','val510','val511','val512','val513','val514','val515','val516','val517','val518','val519','val520','val521','val522','val523','val524','val525','val526','val527','val528','val529','val530','val531','val532','val533','val534','val535','val536','val537','val538','val539','val540','val541','val542','val543','val544','val545','val546','val547','val548','val549','val550','val551','val552','val553','val554','val555','val556','val557','val558','val559','val560','val561','val562','val563','val564','val565','val566','val567','val568','val569','val570','val571','val572','val573','val574','val575','val576','val577','val578','val579','val580','val581','val582','val583','val584','val585','val586','val587','val588','val589','val590','val591','val592','val593','val594','val595','val596','val597','val598','val599','val600','val601','val602','val603','val604','val605','val606','val607','val608','val609','val610','val611','val612','val613','val614','val615','val616','val617','val618','val619','val620','val621','val622','val623','val624','val625','val626','val627','val628','val629','val630','val631','val632','val633','val634','val635','val636','val637','val638','val639','val640','val641','val642','val643','val644','val645','val646','val647','val648','val649','val650','val651','val652','val653','val654','val655','val656','val657','val658','val659','val660','val661','val662','val663','val664','val665','val666','val667','val668','val669','val670','val671','val672','val673','val674','val675','val676','val677','val678','val679','val680','val681','val682','val683','val684','val685','val686','val687','val688','val689','val690','val691','val692','val693','val694','val695','val696','val697','val698','val699');";


	private PreparedStatement insertStmt;

	public Test() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		
		Cluster cluster = Cluster.builder()
				.addContactPoints(contactPointsStr.split(","))
				.withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE))
				.build();
		this.session = cluster.connect();

		insertStmt = session.prepare(insert);
		logger.info("Cluster and Session created.");

		Timer timer = new Timer();
		timer.start();
		insertWideRowsAsync(NO_OR_ROWS);	
		timer.end();
		logger.info("Wide row test finished in " + timer.getTimeTakenSeconds() + " secs" + (NO_OR_ROWS/timer.getTimeTakenSeconds()) + " a sec");		

		session.close();
		cluster.close();
	}
	
	private void insertWideRowsAsync(int noOfRows) {
		BoundStatement boundStmt = new BoundStatement(insertStmt);
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

		int count=0;
		long start = System.currentTimeMillis();
		
		
		for (int i = 0; i < noOfRows; i++){
			boundStmt.bind("id-async-" + (i+1));
			results.add(session.executeAsync(boundStmt));
			
			count++;
			
			if (count % 10 == 0){
				logger.info("Inserted " + count + " rows");
				
				boolean wait = true;
				while(wait){			
					//start with getting out, if any results are not done, wait is true.
					wait = false;			
					for (ResultSetFuture result : results){
						
						try {
							result.get();
						} catch (InterruptedException e) {
							e.printStackTrace();
							throw new RuntimeException(e);
						} catch (ExecutionException e) {
							e.printStackTrace();
							throw new RuntimeException(e);
						}
						
						if (!result.isDone()){
							wait = true;
							break;
						}			
					}
				}
			}
		}
		
		boolean wait = true;
		while(wait){			
			//start with getting out, if any results are not done, wait is true.
			wait = false;			
			for (ResultSetFuture result : results){
				
				try {
					result.get();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
				
				if (!result.isDone()){
					wait = true;
					break;
				}			
			}
		}
		
		logger.info("Inserted (Async) " + noOfRows + " rows in " + (System.currentTimeMillis() - start) + "ms");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Test();
	}
}
