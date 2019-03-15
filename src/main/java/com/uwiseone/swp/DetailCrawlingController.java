package com.uwiseone.swp;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class DetailCrawlingController {

	private final String URL_DOMAIN = "http://sminfo.mss.go.kr";
	private final String URL_DETAIL = "/gc/ei/GEI005R0.do";

	public static Connection getConnection() {
		Connection conn = null;

		try {
			String url = "jdbc:oracle:thin:@10.XXX.XXX.XXX:1521:XXX";
			String id = "XXX";
			String pw = "XXX";

			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection(url,id,pw);
			System.out.println("Database 연결되었습니다.");
		} catch(Exception e) {
			System.out.println("Database 연결 중 오류가 발생하였습니다.");
			e.printStackTrace();
		}

		return conn;
	}

	private CorpDetailEntity execute(String kedcd) throws IOException {
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("kedcd",    kedcd); // 기업고유번호
		paramMap.put("cmMenuId", "441010100");

		Elements root;
		Document doc = Jsoup.connect(URL_DOMAIN + URL_DETAIL)
								 .data(paramMap)
								 .timeout(60000) // 1분
								 .post();
		root = doc.select("div.companyInfoTable");

		// 매출
		StringBuffer salesAmt = new StringBuffer();
		Elements salesElement = root.select("dd.statements dl.tableRight");
		salesAmt.append(salesElement.select("tbody tr").first().select("td").get(0).text()).append("/");
		salesAmt.append(salesElement.select("tbody tr").first().select("td").get(1).text()).append("/");
		salesAmt.append(salesElement.select("tbody tr").first().select("td").get(2).text());

		// 종업원 수
		String empCount = root.select("table tr td[headers=row09]").text();
		// 홈페이지
		String homepage = root.select("table tr td[headers=row12]").text();
		// 대표전화
		String telNo = root.select("table tr td[headers=row14]").text();
		// 주소
		String address1 = root.select("table tr td[headers=row13]").get(0).text();
		String address2 = root.select("table tr td[headers=row13]").get(1).text();
		//특이사항
		String bigo = root.select("table tr td[headers=row16]").text();
		//회사소개
		String corpIntro = root.select("table tr td[headers=row17]").text();


		CorpDetailEntity entity = new CorpDetailEntity();
		entity.setCorpPk(kedcd);
		entity.setAmt(salesAmt.toString());
		entity.setEmpCnt(empCount);
		entity.setHomepage(homepage);
		entity.setTelno(telNo);
		entity.setAddr1(address1);
		entity.setAddr2(address2);
		entity.setBigo(bigo);
		entity.setCorpIntro(corpIntro);

		return entity;
	}

	private List<CorpEntity> getCorpList() {
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement pstmt = null;

		List<CorpEntity> list = null;

		StringBuilder query = null;
		try {
			conn = getConnection();
			query = new StringBuilder();
			query.append(" SELECT CORP_PK FROM SRADMIN.T_CORP WHERE SUCCESS_YN = 'N' ");
			pstmt = conn.prepareStatement(query.toString());
			rs = pstmt.executeQuery();

			list = new ArrayList<CorpEntity>();
			CorpEntity entity = null;
			while(rs.next()) {
				entity = new CorpEntity();
				entity.setCorpPk(rs.getString("CORP_PK"));
				list.add(entity);
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(rs != null) try{rs.close();}catch(SQLException sqle){}
			if(pstmt != null) try{pstmt.close();}catch(SQLException sqle){}
			if(conn != null) try{conn.close();}catch(SQLException sqle){}
		}

		return list;
	}

	private void addDatabase(CorpDetailEntity entity) {
		Connection conn = null;
		PreparedStatement pstmt = null;

		StringBuilder query = new StringBuilder();
		query.append("INSERT INTO SRADMIN.T_CORP_DETAIL( \n");
		query.append("    CORP_PK,                       \n");
		query.append("    AMT,                           \n");
		query.append("    EMP_CNT,                       \n");
		query.append("    HOMEPAGE,                      \n");
		query.append("    TELNO,                         \n");
		query.append("    ADDR1,                         \n");
		query.append("    ADDR2,                         \n");
		query.append("    BIGO,                          \n");
		query.append("    CORP_INTRO                     \n");
		query.append(") VALUES(                          \n");
		query.append("    ?,?,?,?,?,?,?,?,?              \n");
		query.append(")                                  \n");

		try {
			conn = getConnection();
			pstmt = conn.prepareStatement(query.toString());

			pstmt.setString(1, entity.getCorpPk());
			pstmt.setString(2, entity.getAmt());
			pstmt.setString(3, entity.getEmpCnt());
			pstmt.setString(4, entity.getHomepage());
			pstmt.setString(5, entity.getTelno());
			pstmt.setString(6, entity.getAddr1());
			pstmt.setString(7, entity.getAddr2());
			pstmt.setString(8, entity.getBigo());
			pstmt.setString(9, entity.getCorpIntro());

			int result = pstmt.executeUpdate();
			if(result < 1) {
				System.out.println("기업정보 저장 중 오류가 발생하였습니다:" + entity.toString());
			}

		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(pstmt != null) try{pstmt.close();}catch(SQLException sqle){}
			if(conn != null) try{conn.close();}catch(SQLException sqle){}
		}
	}

	private void updateDatabase() {
		Connection conn = null;
		PreparedStatement pstmt = null;

		StringBuilder query = new StringBuilder();
		query.append("UPDATE SRADMIN.T_CORP SET 					\n");
		query.append("	SUCCESS_YN = 'Y' 			 				\n");
		query.append("WHERE CORP_PK IN (							\n");
		query.append("    SELECT CORP_PK FROM SRADMIN.T_CORP_DETAIL	\n");
		query.append(") 											\n");

		try {
			conn = getConnection();
			pstmt = conn.prepareStatement(query.toString());

			System.out.println("==========>> 기업정보 업데이트 시작");
			int resultCnt = pstmt.executeUpdate();
			System.out.println("==========>> 기업정보 업데이트("+resultCnt+") 종료");

		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(pstmt != null) try{pstmt.close();}catch(SQLException sqle){}
			if(conn != null) try{conn.close();}catch(SQLException sqle){}
		}
	}

	public static void main(String[] args) {
		DetailCrawlingController detailCrawlingController = new DetailCrawlingController();

		// 크롤링 대상 기업정보 조회
		List<CorpEntity> corpList = detailCrawlingController.getCorpList();
		System.out.println("기업정보 조회완료 : " + corpList.size());

		// 크롤링 결과 메모리 적재
		CorpDetailEntity corpDetailEntity = null;
		for(CorpEntity entity : corpList) {
			try {
				Thread.sleep(10*1000); //10초
				System.out.println("기업정보 크롤링을 시작 : " + entity.getCorpPk());
				corpDetailEntity = detailCrawlingController.execute(entity.getCorpPk());

				// 크롤링 결과 INSERT
				detailCrawlingController.addDatabase(corpDetailEntity);
				System.out.println("크롤링 정보를 DB에 적재완료 : " + entity.getCorpPk());

				// 크롤링 완료된 기업 정보 업데이트
				detailCrawlingController.updateDatabase();
				System.out.println("크롤링 완료된 기업 정보 업데이트 완료 : " + entity.getCorpPk());
			} catch(Exception e) {
				e.printStackTrace();
			}
		}

	}
}
