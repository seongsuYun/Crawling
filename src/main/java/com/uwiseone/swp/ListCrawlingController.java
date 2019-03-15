package com.uwiseone.swp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ListCrawlingController {

	private final String URL_DOMAIN = "http://sminfo.mss.go.kr";
	private final String URL_LIST   = "/gc/ei/GEI001R0.do";

	private static String[] arrEmp = {"10^20", "20^50", "50^100", "100^500"};
	private static String[] arrSaleAmt = {"500^1000", "1000^2000", "2000^5000", "5000^10000", "10000^50000", "50000^99999999"};
	
	private static int PAGE_NO = 1;
	private static String EMP = "";
	private static String SALEAMT = "";
	
	private void execute() throws IOException {
		System.out.println("====================["+PAGE_NO+" 페이지]=====================");
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("cmMenuId",           "441010100");
		paramMap.put("clickcontrol",       "disable");
		paramMap.put("iGB",                "1");
		paramMap.put("locSrchCd",          "2");
		paramMap.put("sidoCd",             "09");	// 시도코드
		paramMap.put("sidoNm",             "경기");	// 시도명
		paramMap.put("iqFlag",             "S");
		paramMap.put("cmQueryOption",      "07");
		paramMap.put("cmTotalRowCount",    "483");
		paramMap.put("cmPageNo",           String.valueOf(PAGE_NO));	// 페이지 번호
		paramMap.put("cmSortOption",       "0");
		paramMap.put("tITLESortOption",    "2");
		paramMap.put("bZNOSortOption",     "2");
		paramMap.put("cmQueryOptionCombo", "00");
		paramMap.put("iSido",              "09^경기");
		paramMap.put("emCn",               EMP);	// 종업원수
		paramMap.put("sam",                SALEAMT);	// 매출규모
		paramMap.put("cmRowCountPerPage",  "500");		// 페이지당 노출수
		
		/* 종업원수
		<option value="5^10" selected="selected">5~9명</option>
		<option value="10^20">10~19명</option>
		<option value="20^50">20~49명</option>
		<option value="50^100">50~99명</option>
		<option value="100^500">100~499명</option>
		<option value="500^2147483647">500명이상</option>
		*/
		
		/* 매출규모
		<option value="500^1000" selected="selected">5~9억</option>
		<option value="1000^2000">10~19억</option>
		<option value="2000^5000">20~49억</option>
		<option value="5000^10000">50~99억</option>
		<option value="10000^50000">100~499억</option>
		<option value="50000^99999999">500억이상</option>
		*/
		
		Elements root;
		Document doc = Jsoup.connect(URL_DOMAIN + URL_LIST)
								 .data(paramMap)
								 .timeout(30000) // 30초
								 .post();
		root = doc.select("div.choice_table_wrap tbody tr");
		
		Element child;
		
		for(int i=0; i<root.size(); i++) {
			child = root.get(i);
			// 기업명& 고유번호 추출
			String corpSeq = child.select("td").first().select("a").attr("onclick").toString().substring(23, 33);
			String corpName = child.select("td").first().select("a").select("strong").text();
			
			System.out.println("종업원수:" + paramMap.get("emCn") + "/" + "매출규모:" + paramMap.get("sam") + "/" + corpSeq + " / " + corpName);
		}
		
		++PAGE_NO;
	}
	
	public static void main(String[] args) {
		int defaultLoopCnt = 100;
		
		ListCrawlingController listCrawlingController = new ListCrawlingController();
		
		for(String emp : arrEmp) {
			EMP = emp;
			for(String saleAmt : arrSaleAmt) {
				PAGE_NO = 1;
				SALEAMT = saleAmt;
				
				try {
					for(int i=1; i<=defaultLoopCnt; i++) {
						listCrawlingController.execute();
						Thread.sleep(60*1000);
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	
	}
}
