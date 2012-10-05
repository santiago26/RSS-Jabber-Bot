import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//import java.text.DateFormat;
//import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.sun.syndication.feed.synd.SyndCategoryImpl;
import com.sun.syndication.feed.synd.SyndContent;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

import rss2bb.sec.*;

class database
{
	public static final Logger LOG=Logger.getLogger(database.class);
	Connection conn;
	Statement st = null;
	String str = "Test connect to SQLite\n\n";
	String dbConn = account.dbConn;
	private static Object Mutex;
	private long MAX_STANZAS = 32500;
	
 	public database() 
	{
		try
		{
			LOG.info("New DB");
			Mutex = new Object();
			System.setProperty("sqlite.purejava", "true");
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection(dbConn);
			st = conn.createStatement();
			st.executeUpdate("CREATE TABLE IF NOT EXISTS USERS (User_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, Jabber TEXT NOT NULL, Is_conf INTEGER NOT NULL DEFAULT(0), BB INTEGER NOT NULL DEFAULT(1));");
			//LOG.info("USERS created...");
			st.executeUpdate("CREATE TABLE IF NOT EXISTS RSS (RSS_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, RSS_link TEXT NOT NULL, RSS_last_timestamp INTEGER NOT NULL DEFAULT(0), RSS_last_thingy TEXT DEFAULT(NULL), RSS_by_cat INTEGER NOT NULL DEFAULT(0), RSS_last_content BLOB DEFAULT(NULL), RSS_lastbb_content BLOB DEFAULT(NULL), Needs_syntax_recheck INTEGER NOT NULL DEFAULT(0), Syntax_error BLOB DEFAULT(NULL));");
			//LOG.info("RSS created...");
			st.executeUpdate("CREATE TABLE IF NOT EXISTS SUBS (Sub_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, User_id INTEGER NOT NULL, RSS_id INTEGER NOT NULL, Sub_pause INTEGER NOT NULL DEFAULT(0), FOREIGN KEY(User_id) REFERENCES USERS(User_id), FOREIGN KEY(RSS_id) REFERENCES RSS(RSS_id));");
			//LOG.info("Subscriptions created...");
			st.executeUpdate("CREATE TABLE IF NOT EXISTS CONF (Entry_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, User_id INTEGER NOT NULL, User TEXT NOT NULL, FOREIGN KEY(User_id) REFERENCES USERS(User_id));");
			//LOG.info("ConfControl created...");
		}catch(Exception e){LOG.error("ERROR_SQL:",e);}
	}
	//New user: INSERT INTO USERS (Jabber) VALUES ('<jabber>');
	//New RSS: INSERT INTO RSS (RSS_link) VALUES ('<RSS_URI>');
	//New Sub: INSERT INTO SUBS (User_id, RSS_id) VALUES(<User>,<RSS>);
	//New CA: INSERT INTO CONF (User_id, User) VALUES(<User>,<Nickname>);
	
	//Получаем все RSS_id каналов из базы данных
	public List<Long> listRSSFeeds()
	{
		System.out.println("Select all RSS feeds.");
		List<Long> rss_list = new ArrayList<Long>();
		try 
		{
			synchronized (Mutex) {
				ResultSet rs = st.executeQuery("SELECT RSS.RSS_id FROM RSS JOIN SUBS ON SUBS.RSS_id=RSS.RSS_id WHERE Sub_pause=0 GROUP BY RSS.RSS_id ORDER BY RSS.RSS_id ASC;");
				while(rs.next()){rss_list.add(rs.getLong("RSS_id"));}
				rs.close();
				Mutex.notify();
			}
		}catch(Exception e){LOG.error("ERROR_SQL:",e);}
		System.out.println("RSSlist built");
		return rss_list;
	}
		
	//Получаем все связки подписок
	public List<String> listSubs() {
		System.out.println("Select all Subs.");
		List<String> subs_list = new ArrayList<String>();
		try {
			synchronized (Mutex) {
				ResultSet rs = st.executeQuery("SELECT Sub_id, RSS_id, Jabber FROM SUBS INNER JOIN USERS ON USERS.User_id=SUBS.User_id;");
				while(rs.next()){subs_list.add(""+rs.getLong("Sub_id")+":"+rs.getLong("RSS_id")+":"+rs.getString("Jabber"));}
				rs.close();
				Mutex.notify();
			}
		}catch(Exception e){LOG.error("ERROR_SQL:",e);}
		System.out.println("Subslist built");
		return subs_list;
	}
	
	//Получаем все MJID подписанных конференций
 	public List<String> listConf() {
		System.out.println("Select all Conferences.");
		List<String> conf_list = new ArrayList<String>();
		try {
			synchronized (Mutex) {
				ResultSet rs = st.executeQuery("SELECT Jabber FROM USERS WHERE Is_conf=1;");
				while(rs.next()){conf_list.add(rs.getString("Jabber"));}
				rs.close();
				Mutex.notify();
			}
		}catch(Exception e){LOG.error("ERROR_SQL:",e);}
		System.out.println("Conflist built");
		return conf_list;
	}
	
	//Получаем все MJID подписанных конференций
	public List<String> listUsers() {
		System.out.println("Select all Users.");
		List<String> conf_list = new ArrayList<String>();
		try {
			synchronized (Mutex) {
				ResultSet rs = st.executeQuery("SELECT Jabber FROM USERS WHERE Is_conf=0;");
				while(rs.next()){conf_list.add(rs.getString("Jabber"));}
				rs.close();
				Mutex.notify();
			}
		}catch(Exception e){LOG.error("ERROR_SQL:",e);}
		System.out.println("Userlist built");
		return conf_list;
	}
	
	//Получаем все MJID подписанных конференций
	public List<Long> listErrors() {
		System.out.println("Select all Errors.");
		List<Long> error_list = new ArrayList<Long>();
		try {
			synchronized (Mutex) {
				ResultSet rs = st.executeQuery("SELECT RSS_id FROM RSS WHERE Needs_syntax_recheck=1;");
				while(rs.next()){error_list.add(rs.getLong("RSS_id"));}
				rs.close();
				Mutex.notify();
			}
		}catch(Exception e){LOG.error("ERROR_SQL:",e);}
		System.out.println("Errorlist built");
		return error_list;
	}
	
	//Проверяем новые записи в ленте
	public List<String> getNew(long RSS_id, boolean forced)
	{
		String msg = "";//Переменная для формирования сообщения в bb кодах
		String msg_bboff = "";//Переменная для формирования сообщения без bb кодов
		List<String> mesbbCol = new ArrayList<String>();
		List<String> mesCol = new ArrayList<String>();
		List<String> message = new ArrayList<String>();
		//Получаем заголовоки ленты
		//System.out.println("Start GetNew "+RSS_id);
		try
		{
			//Загружаем инфу о текущей ленте
			String RSS_link;
			Long RSS_category;
			String RSS_last_thingy;
			Long RSS_last_timestamp;
			boolean Already_failed;
			boolean NSR;
			synchronized (Mutex) {
				ResultSet rs;
				//System.out.println("Entered Mutex Init");
				rs=st.executeQuery("SELECT * FROM RSS WHERE RSS_id="+RSS_id+";");

				if (!(rs.next()))
				{
					LOG.fatal("FUCK THIS SYSTEM!");
					//System.out.println("Left Mutex Init");
					Mutex.notify();
					return null;
				}
				if ((rs.getLong("Needs_syntax_recheck")==1)&&(!forced))
				{
					//System.out.println("Left Mutex Init");
					Mutex.notify();
					message.add(msg);
					message.add(msg_bboff);
					return message;
				}

				//Вгружаем инфу о ленте в переменные (Да, мне лень дергать rs по каждому поводу)
				RSS_link=rs.getString("RSS_link");
				RSS_category=rs.getLong("RSS_by_cat");
				RSS_last_thingy=rs.getString("RSS_last_thingy");
				RSS_last_timestamp=rs.getLong("RSS_last_timestamp");
				rs.getBytes("Syntax_error");
				Already_failed=(forced || !rs.wasNull());
				NSR=(rs.getLong("Needs_syntax_recheck")==1);
				rs.close();
				//System.out.println("Left Mutex Init");
				Mutex.notify();
			}
			

			//Подключаемся к интернетам
			//LOG.info(RSS_id+"("+RSS_link+")");
			URL feedUrl = new URL(RSS_link);
			URLConnection feedCon = feedUrl.openConnection();
			feedCon.setConnectTimeout(10000);
			feedCon.setReadTimeout(10000);
			try
			{
				SyndFeedInput input = new SyndFeedInput();
				//XmlReader reader = new XmlReader(feedUrl);//FREEZE HERE. Connection freeze
				XmlReader reader = new XmlReader(feedCon);
				try
				{
					SyndFeed RSS_feed = input.build(reader);
					//System.out.println("Entered Stream");
					//Большой разнос по категории ленты для проверки наличия обновлений
					//0-timestamp,1-guid/uri,2-link,3-title;
					switch (RSS_category.intValue()){
					case 0:{//Означает, что в каждом посте есть дата публикации или обновления(что б вас через колено)
						boolean updated=false;
						if (( (SyndEntry)RSS_feed.getEntries().get(0) ).getPublishedDate() ==null) updated=true;
						if (
								(
										(
												(SyndEntry)RSS_feed.getEntries().get(0)
										).getPublishedDate() ==null
								)
						&&
								(
										(
												(SyndEntry)RSS_feed.getEntries().get(0)
										).getUpdatedDate() ==null
								) 
						){
							//Категория неверна. Заморозка и выход.
							synchronized (Mutex) {
								System.out.println("Entered Mutex Error");
								PreparedStatement prepError = conn.prepareStatement("UPDATE RSS SET Needs_syntax_recheck=1, Syntax_error=? WHERE RSS_id="+RSS_id+";");
								System.out.println("prepError");
								String Syntax_error="";
								Syntax_error+="ERROR_DATE: Wrong category (0). PubDate not found."+((SyndEntry)RSS_feed.getEntries().get(0)).toString();
								try {
									prepError.setBytes(1, Syntax_error.getBytes("UTF-8"));
								}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
								prepError.execute();
								reader.close();
								System.out.println("Left Mutex Error");
								Mutex.notify();
							}
							return null;
						}
						Long Update = (!updated)?((SyndEntry)RSS_feed.getEntries().get(0)).getPublishedDate().getTime():((SyndEntry)RSS_feed.getEntries().get(0)).getUpdatedDate().getTime();
						boolean UpdateDone=false;
						if (Update>RSS_last_timestamp)
						{
							//System.out.println("Start refresh");
							//Получаем новые ленты
							for (Object object : RSS_feed.getEntries()){
								SyndEntry entry = (SyndEntry) object;
								
								if (((!updated)?entry.getPublishedDate():entry.getUpdatedDate()) ==null) continue;
								if (((!updated)?entry.getPublishedDate().getTime():entry.getUpdatedDate().getTime()) <=RSS_last_timestamp){
									UpdateDone=true;
									break;
								}

								String postTitle = entry.getTitle();
								if(postTitle == null){postTitle = entry.getLink();}

								Date postUpdate = (!updated)?entry.getPublishedDate():entry.getUpdatedDate();
								String postDate = postUpdate.toString();

								String postLink = entry.getLink();

								@SuppressWarnings("unchecked")
								List<SyndCategoryImpl> postCategories = entry.getCategories();

								String postAuthor = entry.getAuthor().toString();

								if ((postAuthor==null)||(postAuthor==""))
								{
									postAuthor="UNKNOWN";
									try
									{
										postAuthor=RSS_feed.getAuthor().toString();
									}catch(Exception e){if(e.getMessage() == null){postAuthor = "UNKNOWN";}}
									if ((postAuthor==null)||(postAuthor==""))
									{postAuthor="UNKNOWN";}
								}
								
								msg_bboff +=postTitle+"\n";
								msg_bboff +=postLink+"\n";	                            	
								msg +="[quote]";
								msg +="[b][url=\""+postLink+"\"]"+postTitle+"[/url][/b]\n";
								
								msg +="[i][size=\"8\"]";
								if (!postAuthor.equals("UNKNOWN")){
									msg_bboff +="Автор: "+postAuthor+" ";
									msg +="Автор: "+postAuthor+" ";
								}
								msg_bboff +="("+postDate+")\n";
								msg +="("+postDate+")[/size][/i]\n";
								msg +="[hr]";

								for(Object contentsObj : entry.getContents())
								{
									SyndContent contents = (SyndContent)contentsObj;
									msg +=html2bb.parse(contents.getValue());
									msg_bboff +=html2bb.parse_bboff(contents.getValue());
								}
								
								//Добавляем перенос строки, так как может быть контент и в getContents, и в getDescription
								if(entry.getContents().size() != 0)
								{
									if((entry.getDescription())!= null)
									{
										msg +="\n\n";
										msg_bboff +="\n---\n";
									}
								}

								SyndContent content = entry.getDescription();
								if(content != null)
								{
									msg +=html2bb.parse(content.getValue());
									msg_bboff +=html2bb.parse_bboff(content.getValue());
								}
								
								if ((postCategories!=null)&&(!postCategories.isEmpty()))
								{
									msg_bboff +="\nТеги: ";
									msg +="\n[i][size=\"8\"]";
									for (SyndCategoryImpl Cat : postCategories)
									{
										msg_bboff +=Cat.getName()+", ";
										msg +=Cat.getName()+", ";
									}
									msg =msg.substring(0, msg.length()-2);
									msg_bboff =msg_bboff.substring(0, msg_bboff.length()-2);
									msg +="[/size][/i]";
								}

								msg +="[/quote]\n";
								msg_bboff +="\n------------------------------------------------------------------\n";

								mesbbCol.add(msg);
								mesCol.add(msg_bboff);
								msg="";
								msg_bboff="";
								//System.out.println("Post built");
							}
							UpdateDone=true;
						}
						else {
							//Обновлений нету
						}
						if ((UpdateDone)||(Already_failed)){
							//Собираем последний пост в сообщение
							String Content="";
							String Contentbb="";
							{
								SyndEntry entry = (SyndEntry) RSS_feed.getEntries().get(0);

								String postTitle = entry.getTitle();
								if(postTitle == null){postTitle = entry.getLink();}

								Date postUpdate = (!updated)?entry.getPublishedDate():entry.getUpdatedDate();
								String postDate = postUpdate.toString();

								String postLink = entry.getLink();

								@SuppressWarnings("unchecked")
								List<SyndCategoryImpl> postCategories = entry.getCategories();

								String postAuthor = entry.getAuthor().toString();

								if ((postAuthor==null)||(postAuthor==""))
								{
									postAuthor="UNKNOWN";
									try
									{
										postAuthor=RSS_feed.getAuthor().toString();
									}catch(Exception e){if(e.getMessage() == null){postAuthor = "UNKNOWN";}}
									if ((postAuthor==null)||(postAuthor==""))
									{postAuthor="UNKNOWN";}
								}
								
								Content +=postTitle+"\n";
								Content +=postLink+"\n";	                            	
								Contentbb +="[quote]";
								Contentbb +="[b][url=\""+postLink+"\"]"+postTitle+"[/url][/b]\n";

								Contentbb +="[i][size=\"8\"]";
								if (!postAuthor.equals("UNKNOWN")){
									Content +="Автор: "+postAuthor+" ";
									Contentbb +="Автор: "+postAuthor+" ";
								}
								Content +="("+postDate+")\n";
								Contentbb +="("+postDate+")[/size][/i]\n";
								Contentbb +="[hr]";

								for(Object contentsObj : entry.getContents())
								{
									SyndContent contents = (SyndContent)contentsObj;
									Contentbb +=html2bb.parse(contents.getValue());
									Content +=html2bb.parse_bboff(contents.getValue());
								}

								//Добавляем перенос строки, так как может быть контент и в getContents, и в getDescription
								if(entry.getContents().size() != 0)
								{
									if((entry.getDescription())!= null)
									{
										Contentbb +="\n\n";
										Content +="\n---\n";
									}
								}

								SyndContent content = entry.getDescription();
								if(content != null)
								{
									Contentbb +=html2bb.parse(content.getValue());
									Content +=html2bb.parse_bboff(content.getValue());
								}

								if ((postCategories!=null)&&(!postCategories.isEmpty()))
								{
									Content +="\nТеги: ";
									Contentbb +="\n[i][size=\"8\"]";
									for (SyndCategoryImpl Cat : postCategories)
									{
										Content +=Cat.getName()+", ";
										Contentbb +=Cat.getName()+", ";
									}
									Contentbb =Contentbb.substring(0, Contentbb.length()-2);
									Content =Content.substring(0, Content.length()-2);
									Contentbb +="[/size][/i]";
								}
								
								Contentbb +="[/quote]\n";
								Content +="\n------------------------------------------------------------------\n";
							}
							//Сохраняем новую дату публикации
							synchronized (Mutex) {
								//System.out.println("Entered Mutex refresh");
								PreparedStatement prepUpdate = conn.prepareStatement("UPDATE RSS SET RSS_last_timestamp="+Update+", RSS_last_content=?, RSS_lastbb_content=?, Syntax_error=NULL, Needs_syntax_recheck=0 WHERE RSS_id="+RSS_id+";");
								try {
									prepUpdate.setBytes(1, Content.getBytes("UTF-8"));
									prepUpdate.setBytes(2, Contentbb.getBytes("UTF-8"));
								}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
								prepUpdate.execute();
								//System.out.println("Left Mutex refresh");
								Mutex.notify();
							}
						}
					}break;//Означает, что дату публикации тырим из ленты, если возможно
					case 1:{
						if (((SyndEntry)(RSS_feed.getEntries().get(0))).getUri()==null){
							//Категория неверна. Заморозка и выход.
							synchronized (Mutex) {
								System.out.println("Entered Mutex Error");
								PreparedStatement prepError = conn.prepareStatement("UPDATE RSS SET Needs_syntax_recheck=1, Syntax_error=? WHERE RSS_id="+RSS_id+";");
								System.out.println("prepError");
								String Syntax_error="";
								Syntax_error+="ERROR_DATE: Wrong category (1). PubDate not found.";
								try {
									prepError.setBytes(1, Syntax_error.getBytes("UTF-8"));
								}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
								prepError.execute();
								reader.close();
								System.out.println("Left Mutex Error");
								Mutex.notify();
							}
							return null;
						}
						String Update = ((SyndEntry)RSS_feed.getEntries().get(0)).getUri();
						Date postUpdate = RSS_feed.getPublishedDate();
						String postDate;
						if (postUpdate==null) {
							postDate = "???";
						}
						else {
							postDate = postUpdate.toString();
						}
						//System.out.println("Got Date");
						boolean UpdateDone=false;
						if (!Update.equals(RSS_last_thingy))
						{
							//System.out.println("Start refresh");
							//Получаем новые ленты
							for (Object object : RSS_feed.getEntries()){
								SyndEntry entry = (SyndEntry) object;

								if (entry.getUri().equals(RSS_last_thingy)){
									UpdateDone=true;
									break;
								}
								String postTitle = entry.getTitle();
								if(postTitle == null){postTitle = entry.getLink();}

								String postLink = entry.getLink();

								@SuppressWarnings("unchecked")
								List<SyndCategoryImpl> postCategories = entry.getCategories();

								String postAuthor = entry.getAuthor().toString();

								if ((postAuthor==null)||(postAuthor==""))
								{
									postAuthor="UNKNOWN";
									try
									{
										postAuthor=RSS_feed.getAuthor().toString();
									}catch(Exception e){if(e.getMessage() == null){postAuthor = "UNKNOWN";}}
									if ((postAuthor==null)||(postAuthor==""))
									{postAuthor="UNKNOWN";}
								}

								msg_bboff +=postTitle+"\n";
								msg_bboff +=postLink+"\n";	                            	
								msg +="[quote]";
								msg +="[b][url=\""+postLink+"\"]"+postTitle+"[/url][/b]\n";

								msg +="[i][size=\"8\"]";
								if (!postAuthor.equals("UNKNOWN")){
									msg_bboff +="Автор: "+postAuthor+" ";
									msg +="Автор: "+postAuthor+" ";
								}
								msg_bboff +="("+postDate+")\n";
								msg +="("+postDate+")[/size][/i]\n";
								msg +="[hr]";

								for(Object contentsObj : entry.getContents())
								{
									SyndContent contents = (SyndContent)contentsObj;
									msg +=html2bb.parse(contents.getValue());
									msg_bboff +=html2bb.parse_bboff(contents.getValue());
								}

								//Добавляем перенос строки, так как может быть контент и в getContents, и в getDescription
								if(entry.getContents().size() != 0)
								{
									if((entry.getDescription())!= null)
									{
										msg +="\n\n";
										msg_bboff +="\n---\n";
									}
								}

								SyndContent content = entry.getDescription();
								if(content != null)
								{
									msg +=html2bb.parse(content.getValue());
									msg_bboff +=html2bb.parse_bboff(content.getValue());
								}

								if ((postCategories!=null)&&(!postCategories.isEmpty()))
								{
									msg_bboff +="\nТеги: ";
									msg +="\n[i][size=\"8\"]";
									for (SyndCategoryImpl Cat : postCategories)
									{
										msg_bboff +=Cat.getName()+", ";
										msg +=Cat.getName()+", ";
									}
									msg =msg.substring(0, msg.length()-2);
									msg_bboff =msg_bboff.substring(0, msg_bboff.length()-2);
									msg +="[/size][/i]";
								}

								msg +="[/quote]\n";
								msg_bboff +="\n------------------------------------------------------------------\n";

								mesbbCol.add(msg);
								mesCol.add(msg_bboff);
								msg="";
								msg_bboff="";
								//System.out.println("Post built");
							}
							UpdateDone=true;
						}
						else {
							//Обновлений нету
						}
						if ((UpdateDone)||(Already_failed)){
							//Собираем последний пост в сообщение
							String Content="";
							String Contentbb="";
							{
								SyndEntry entry = (SyndEntry) RSS_feed.getEntries().get(0);

								String postTitle = entry.getTitle();
								if(postTitle == null){postTitle = entry.getLink();}

								String postLink = entry.getLink();

								@SuppressWarnings("unchecked")
								List<SyndCategoryImpl> postCategories = entry.getCategories();

								String postAuthor = entry.getAuthor().toString();

								if ((postAuthor==null)||(postAuthor==""))
								{
									postAuthor="UNKNOWN";
									try
									{
										postAuthor=RSS_feed.getAuthor().toString();
									}catch(Exception e){if(e.getMessage() == null){postAuthor = "UNKNOWN";}}
									if ((postAuthor==null)||(postAuthor==""))
									{postAuthor="UNKNOWN";}
								}

								Content +=postTitle+"\n";
								Content +=postLink+"\n";	                            	
								Contentbb +="[quote]";
								Contentbb +="[b][url=\""+postLink+"\"]"+postTitle+"[/url][/b]\n";

								Contentbb +="[i][size=\"8\"]";
								if (!postAuthor.equals("UNKNOWN")){
									Content +="Автор: "+postAuthor+" ";
									Contentbb +="Автор: "+postAuthor+" ";
								}
								Content +="("+postDate+")\n";
								Contentbb +="("+postDate+")[/size][/i]\n";
								Contentbb +="[hr]";

								for(Object contentsObj : entry.getContents())
								{
									SyndContent contents = (SyndContent)contentsObj;
									Contentbb +=html2bb.parse(contents.getValue());
									Content +=html2bb.parse_bboff(contents.getValue());
								}

								//Добавляем перенос строки, так как может быть контент и в getContents, и в getDescription
								if(entry.getContents().size() != 0)
								{
									if((entry.getDescription())!= null)
									{
										Contentbb +="\n\n";
										Content +="\n---\n";
									}
								}

								SyndContent content = entry.getDescription();
								if(content != null)
								{
									Contentbb +=html2bb.parse(content.getValue());
									Content +=html2bb.parse_bboff(content.getValue());
								}

								if ((postCategories!=null)&&(!postCategories.isEmpty()))
								{
									Content +="\nТеги: ";
									Contentbb +="\n[i][size=\"8\"]";
									for (SyndCategoryImpl Cat : postCategories)
									{
										Content +=Cat.getName()+", ";
										Contentbb +=Cat.getName()+", ";
									}
									Contentbb =Contentbb.substring(0, Contentbb.length()-2);
									Content =Content.substring(0, Content.length()-2);
									Contentbb +="[/size][/i]";
								}

								Contentbb +="[/quote]\n";
								Content +="\n------------------------------------------------------------------\n";
							}
							//Сохраняем новую дату публикации
							//Экран для записи в БД
							synchronized (Mutex) {
								//System.out.println("Entered Mutex Refresh");
								Update=Update.replaceAll("'", "''");
								PreparedStatement prepUpdate = conn.prepareStatement("UPDATE RSS SET RSS_last_thingy='"+Update+"', RSS_last_content=?, RSS_lastbb_content=?, Syntax_error=NULL, Needs_syntax_recheck=0 WHERE RSS_id="+RSS_id+";");
								Update=Update.replaceAll("''", "'");
								try {
									prepUpdate.setBytes(1, Content.getBytes("UTF-8"));
									prepUpdate.setBytes(2, Contentbb.getBytes("UTF-8"));
								}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
								prepUpdate.execute();
								//System.out.println("Left Mutex Refresh");
								Mutex.notify();
							}
						}
					}break;
					case 2:{
						if (((SyndEntry)(RSS_feed.getEntries().get(0))).getLink()==null){
							//Категория неверна. Заморозка и выход.
							synchronized (Mutex) {
								System.out.println("Entered Mutex Error");
								PreparedStatement prepError = conn.prepareStatement("UPDATE RSS SET Needs_syntax_recheck=1, Syntax_error=? WHERE RSS_id="+RSS_id+";");
								System.out.println("prepError");
								String Syntax_error="";
								Syntax_error+="ERROR_DATE: Wrong category (2). PubDate not found.";
								try {
									prepError.setBytes(1, Syntax_error.getBytes("UTF-8"));
								}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
								prepError.execute();
								reader.close();
								System.out.println("Left Mutex Error");
								Mutex.notify();
							}
							return null;
						}
						String Update = ((SyndEntry)RSS_feed.getEntries().get(0)).getLink();
						Date postUpdate = RSS_feed.getPublishedDate();
						String postDate;
						if (postUpdate==null) {
							postDate = "???";
						}
						else {
							postDate = postUpdate.toString();
						}
						//System.out.println("Got Date");
						boolean UpdateDone=false;
						if (!Update.equals(RSS_last_thingy))
						{
							//System.out.println("Start refresh");
							//Получаем новые ленты
							for (Object object : RSS_feed.getEntries()){
								SyndEntry entry = (SyndEntry) object;

								if (entry.getLink().equals(RSS_last_thingy)){
									UpdateDone=true;
									break;
								}

								String postTitle = entry.getTitle();
								if(postTitle == null){postTitle = entry.getLink();}

								String postLink = entry.getLink();

								@SuppressWarnings("unchecked")
								List<SyndCategoryImpl> postCategories = entry.getCategories();

								String postAuthor = entry.getAuthor().toString();

								if ((postAuthor==null)||(postAuthor==""))
								{
									postAuthor="UNKNOWN";
									try
									{
										postAuthor=RSS_feed.getAuthor().toString();
									}catch(Exception e){if(e.getMessage() == null){postAuthor = "UNKNOWN";}}
									if ((postAuthor==null)||(postAuthor==""))
									{postAuthor="UNKNOWN";}
								}

								msg_bboff +=postTitle+"\n";
								msg_bboff +=postLink+"\n";	                            	
								msg +="[quote]";
								msg +="[b][url=\""+postLink+"\"]"+postTitle+"[/url][/b]\n";

								msg +="[i][size=\"8\"]";
								if (!postAuthor.equals("UNKNOWN")){
									msg_bboff +="Автор: "+postAuthor+" ";
									msg +="Автор: "+postAuthor+" ";
								}
								msg_bboff +="("+postDate+")\n";
								msg +="("+postDate+")[/size][/i]\n";
								msg +="[hr]";

								for(Object contentsObj : entry.getContents())
								{
									SyndContent contents = (SyndContent)contentsObj;
									msg +=html2bb.parse(contents.getValue());
									msg_bboff +=html2bb.parse_bboff(contents.getValue());
								}

								//Добавляем перенос строки, так как может быть контент и в getContents, и в getDescription
								if(entry.getContents().size() != 0)
								{
									if((entry.getDescription())!= null)
									{
										msg +="\n\n";
										msg_bboff +="\n---\n";
									}
								}

								SyndContent content = entry.getDescription();
								if(content != null)
								{
									msg +=html2bb.parse(content.getValue());
									msg_bboff +=html2bb.parse_bboff(content.getValue());
								}

								if ((postCategories!=null)&&(!postCategories.isEmpty()))
								{
									msg_bboff +="\nТеги: ";
									msg +="\n[i][size=\"8\"]";
									for (SyndCategoryImpl Cat : postCategories)
									{
										msg_bboff +=Cat.getName()+", ";
										msg +=Cat.getName()+", ";
									}
									msg =msg.substring(0, msg.length()-2);
									msg_bboff =msg_bboff.substring(0, msg_bboff.length()-2);
									msg +="[/size][/i]";
								}

								msg +="[/quote]\n";
								msg_bboff +="\n------------------------------------------------------------------\n";

								mesbbCol.add(msg);
								mesCol.add(msg_bboff);
								msg="";
								msg_bboff="";
								//System.out.println("Post built");
							}
							UpdateDone=true;
						}
						else {
							//Обновлений нету
						}
						if ((UpdateDone)||(Already_failed)){
							//Собираем последний пост в сообщение
							String Content="";
							String Contentbb="";
							{
								SyndEntry entry = (SyndEntry) RSS_feed.getEntries().get(0);

								String postTitle = entry.getTitle();
								if(postTitle == null){postTitle = entry.getLink();}

								String postLink = entry.getLink();

								@SuppressWarnings("unchecked")
								List<SyndCategoryImpl> postCategories = entry.getCategories();

								String postAuthor = entry.getAuthor().toString();

								if ((postAuthor==null)||(postAuthor==""))
								{
									postAuthor="UNKNOWN";
									try
									{
										postAuthor=RSS_feed.getAuthor().toString();
									}catch(Exception e){if(e.getMessage() == null){postAuthor = "UNKNOWN";}}
									if ((postAuthor==null)||(postAuthor==""))
									{postAuthor="UNKNOWN";}
								}

								Content +=postTitle+"\n";
								Content +=postLink+"\n";	                            	
								Contentbb +="[quote]";
								Contentbb +="[b][url=\""+postLink+"\"]"+postTitle+"[/url][/b]\n";

								Contentbb +="[i][size=\"8\"]";
								if (!postAuthor.equals("UNKNOWN")){
									Content +="Автор: "+postAuthor+" ";
									Contentbb +="Автор: "+postAuthor+" ";
								}
								Content +="("+postDate+")\n";
								Contentbb +="("+postDate+")[/size][/i]\n";
								Contentbb +="[hr]";

								for(Object contentsObj : entry.getContents())
								{
									SyndContent contents = (SyndContent)contentsObj;
									Contentbb +=html2bb.parse(contents.getValue());
									Content +=html2bb.parse_bboff(contents.getValue());
								}

								//Добавляем перенос строки, так как может быть контент и в getContents, и в getDescription
								if(entry.getContents().size() != 0)
								{
									if((entry.getDescription())!= null)
									{
										Contentbb +="\n\n";
										Content +="\n---\n";
									}
								}

								SyndContent content = entry.getDescription();
								if(content != null)
								{
									Contentbb +=html2bb.parse(content.getValue());
									Content +=html2bb.parse_bboff(content.getValue());
								}

								if ((postCategories!=null)&&(!postCategories.isEmpty()))
								{
									Content +="\nТеги: ";
									Contentbb +="\n[i][size=\"8\"]";
									for (SyndCategoryImpl Cat : postCategories)
									{
										Content +=Cat.getName()+", ";
										Contentbb +=Cat.getName()+", ";
									}
									Contentbb =Contentbb.substring(0, Contentbb.length()-2);
									Content =Content.substring(0, Content.length()-2);
									Contentbb +="[/size][/i]";
								}

								Contentbb +="[/quote]\n";
								Content +="\n------------------------------------------------------------------\n";
							}
							//Сохраняем новую дату публикации
							synchronized (Mutex) {
								//System.out.println("Entered Mutex Refresh");
								Update=Update.replaceAll("'", "''");
								PreparedStatement prepUpdate = conn.prepareStatement("UPDATE RSS SET RSS_last_thingy='"+Update+"', RSS_last_content=?, RSS_lastbb_content=?, Syntax_error=NULL, Needs_syntax_recheck=0 WHERE RSS_id="+RSS_id+";");
								Update=Update.replaceAll("''", "'");
								try {
									prepUpdate.setBytes(1, Content.getBytes("UTF-8"));
									prepUpdate.setBytes(2, Contentbb.getBytes("UTF-8"));
								}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
								prepUpdate.execute();
								//System.out.println("Left Mutex Refresh");
								Mutex.notify();
							}
						}
					}break;
					case 3:{
						if (((SyndEntry)(RSS_feed.getEntries().get(0))).getTitle()==null){
							//Категория неверна. Заморозка и выход.
							synchronized (Mutex) {
								System.out.println("Entered Mutex Error");
								PreparedStatement prepError = conn.prepareStatement("UPDATE RSS SET Needs_syntax_recheck=1, Syntax_error=? WHERE RSS_id="+RSS_id+";");
								System.out.println("prepError");
								String Syntax_error="";
								Syntax_error+="ERROR_DATE: Wrong category (1). PubDate not found.";
								try {
									prepError.setBytes(1, Syntax_error.getBytes("UTF-8"));
								}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
								prepError.execute();
								reader.close();
								System.out.println("Left Mutex Error");
								Mutex.notify();
							}
							return null;
						}
						String Update = ((SyndEntry)RSS_feed.getEntries().get(0)).getTitle();
						Date postUpdate = RSS_feed.getPublishedDate();
						String postDate;
						if (postUpdate==null) {
							postDate = "???";
						}
						else {
							postDate = postUpdate.toString();
						}
						//System.out.println("Got Date");
						boolean UpdateDone=false;
						if (!Update.equals(RSS_last_thingy))
						{
							//System.out.println("Start refresh");
							//Получаем новые ленты
							for (Object object : RSS_feed.getEntries()){
								SyndEntry entry = (SyndEntry) object;

								if (entry.getTitle().equals(RSS_last_thingy)){
									UpdateDone=true;
									break;
								}

								String postTitle = entry.getTitle();
								//System.out.println("Got title");

								String postLink = entry.getLink();
								if (postLink==null){
									postLink="http://www.google.com/search?q="+postTitle;
								}

								@SuppressWarnings("unchecked")
								List<SyndCategoryImpl> postCategories = entry.getCategories();

								String postAuthor = entry.getAuthor().toString();

								if ((postAuthor==null)||(postAuthor==""))
								{
									postAuthor="UNKNOWN";
									try
									{
										postAuthor=RSS_feed.getAuthor().toString();
									}catch(Exception e){if(e.getMessage() == null){postAuthor = "UNKNOWN";}}
									if ((postAuthor==null)||(postAuthor==""))
									{postAuthor="UNKNOWN";}
								}

								msg_bboff +=postTitle+"\n";
								msg_bboff +=postLink+"\n";	                            	
								msg +="[quote]";
								msg +="[b][url=\""+postLink+"\"]"+postTitle+"[/url][/b]\n";

								msg +="[i][size=\"8\"]";
								if (!postAuthor.equals("UNKNOWN")){
									msg_bboff +="Автор: "+postAuthor+" ";
									msg +="Автор: "+postAuthor+" ";
								}
								msg_bboff +="("+postDate+")\n";
								msg +="("+postDate+")[/size][/i]\n";
								msg +="[hr]";

								for(Object contentsObj : entry.getContents())
								{
									SyndContent contents = (SyndContent)contentsObj;
									msg +=html2bb.parse(contents.getValue());
									msg_bboff +=html2bb.parse_bboff(contents.getValue());
								}

								//Добавляем перенос строки, так как может быть контент и в getContents, и в getDescription
								if(entry.getContents().size() != 0)
								{
									if((entry.getDescription())!= null)
									{
										msg +="\n\n";
										msg_bboff +="\n---\n";
									}
								}

								SyndContent content = entry.getDescription();
								if(content != null)
								{
									msg +=html2bb.parse(content.getValue());
									msg_bboff +=html2bb.parse_bboff(content.getValue());
								}

								if ((postCategories!=null)&&(!postCategories.isEmpty()))
								{
									msg_bboff +="\nТеги: ";
									msg +="\n[i][size=\"8\"]";
									for (SyndCategoryImpl Cat : postCategories)
									{
										msg_bboff +=Cat.getName()+", ";
										msg +=Cat.getName()+", ";
									}
									msg =msg.substring(0, msg.length()-2);
									msg_bboff =msg_bboff.substring(0, msg_bboff.length()-2);
									msg +="[/size][/i]";
								}

								msg +="[/quote]\n";
								msg_bboff +="\n------------------------------------------------------------------\n";

								mesbbCol.add(msg);
								mesCol.add(msg_bboff);
								msg="";
								msg_bboff="";
								//System.out.println("Post built");
							}
							UpdateDone=true;
						}
						else {
							//Обновлений нету
						}
						if ((UpdateDone)||(Already_failed)){
							//Собираем последний пост в сообщение
							String Content="";
							String Contentbb="";
							{
								SyndEntry entry = (SyndEntry) RSS_feed.getEntries().get(0);

								String postTitle = entry.getTitle();
								//System.out.println("Got title");

								String postLink = entry.getLink();
								if (postLink==null){
									postLink="http://www.google.com/search?q="+postTitle;
								}

								@SuppressWarnings("unchecked")
								List<SyndCategoryImpl> postCategories = entry.getCategories();

								String postAuthor = entry.getAuthor().toString();

								if ((postAuthor==null)||(postAuthor==""))
								{
									postAuthor="UNKNOWN";
									try
									{
										postAuthor=RSS_feed.getAuthor().toString();
									}catch(Exception e){if(e.getMessage() == null){postAuthor = "UNKNOWN";}}
									if ((postAuthor==null)||(postAuthor==""))
									{postAuthor="UNKNOWN";}
								}

								Content +=postTitle+"\n";
								Content +=postLink+"\n";	                            	
								Contentbb +="[quote]";
								Contentbb +="[b][url=\""+postLink+"\"]"+postTitle+"[/url][/b]\n";

								Contentbb +="[i][size=\"8\"]";
								if (!postAuthor.equals("UNKNOWN")){
									Content +="Автор: "+postAuthor+" ";
									Contentbb +="Автор: "+postAuthor+" ";
								}
								Content +="("+postDate+")\n";
								Contentbb +="("+postDate+")[/size][/i]\n";
								Contentbb +="[hr]";

								for(Object contentsObj : entry.getContents())
								{
									SyndContent contents = (SyndContent)contentsObj;
									Contentbb +=html2bb.parse(contents.getValue());
									Content +=html2bb.parse_bboff(contents.getValue());
								}

								//Добавляем перенос строки, так как может быть контент и в getContents, и в getDescription
								if(entry.getContents().size() != 0)
								{
									if((entry.getDescription())!= null)
									{
										Contentbb +="\n\n";
										Content +="\n---\n";
									}
								}

								SyndContent content = entry.getDescription();
								if(content != null)
								{
									Contentbb +=html2bb.parse(content.getValue());
									Content +=html2bb.parse_bboff(content.getValue());
								}

								if ((postCategories!=null)&&(!postCategories.isEmpty()))
								{
									Content +="\nТеги: ";
									Contentbb +="\n[i][size=\"8\"]";
									for (SyndCategoryImpl Cat : postCategories)
									{
										Content +=Cat.getName()+", ";
										Contentbb +=Cat.getName()+", ";
									}
									Contentbb =Contentbb.substring(0, Contentbb.length()-2);
									Content =Content.substring(0, Content.length()-2);
									Contentbb +="[/size][/i]";
								}

								Contentbb +="[/quote]\n";
								Content +="\n------------------------------------------------------------------\n";
							}
							//Сохраняем новую дату публикации
							synchronized (Mutex) {
								//System.out.println("Entered Mutex Refresh");
								Update=Update.replaceAll("'", "''");
								PreparedStatement prepUpdate = conn.prepareStatement("UPDATE RSS SET RSS_last_thingy='"+Update+"', RSS_last_content=?, RSS_lastbb_content=?, Syntax_error=NULL, Needs_syntax_recheck=0 WHERE RSS_id="+RSS_id+";");
								Update=Update.replaceAll("''", "'");
								try {
									prepUpdate.setBytes(1, Content.getBytes("UTF-8"));
									prepUpdate.setBytes(2, Contentbb.getBytes("UTF-8"));
								}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
								prepUpdate.execute();
								//System.out.println("Left Mutex Refresh");
								Mutex.notify();
							}
						}
					}break;
					default:{
						LOG.fatal("FUCK THIS SYSTEM!");
						reader.close();
						return null;
					}
					}
				}catch (Exception e)
				{
					if (Already_failed) {
						if (!NSR) synchronized (Mutex) {
							System.out.println("Entered Mutex Error");
							LOG.error(RSS_id+" ("+RSS_link+")");
							System.out.println("ERROR_FEED:"+e.toString());
							PreparedStatement prepError = conn.prepareStatement("UPDATE RSS SET Syntax_error=?, Needs_syntax_recheck=1 WHERE RSS_id="+RSS_id+";");
							System.out.println("prepError");
							String Syntax_error="";
							Syntax_error+="ERROR_FEED:";
							Syntax_error+=e.toString();
							for (StackTraceElement STE : e.getStackTrace()){
								Syntax_error += "\nat "+STE.toString();
							}
							try {
								prepError.setBytes(1, Syntax_error.getBytes("UTF-8"));
							}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
							prepError.execute();
							reader.close();
							System.out.println("Left Mutex Error");
							Mutex.notify();
						}
						return null;
					}
					else {
						synchronized (Mutex) {
							System.out.println("Entered Mutex Warn");
							LOG.warn("Warning: "+RSS_id+" ("+RSS_link+") : ERROR_FEED:",e);
							System.out.println("ERROR_FEED:"+e.toString());
							PreparedStatement prepError = conn.prepareStatement("UPDATE RSS SET Syntax_error=? WHERE RSS_id="+RSS_id+";");
							System.out.println("prepError");
							String Syntax_error="";
							Syntax_error+="ERROR_FEED:";
							Syntax_error+=e.toString();
							for (StackTraceElement STE : e.getStackTrace()){
								Syntax_error += "\nat "+STE.toString();
							}
							try {
								prepError.setBytes(1, Syntax_error.getBytes("UTF-8"));
							}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
							prepError.execute();
							reader.close();
							System.out.println("Left Mutex Warn");
							Mutex.notify();
						}
						message.add(msg);
						message.add(msg_bboff);
						return message;
					}
				}
				reader.close();
			}catch (Exception e)
			{
				if (Already_failed) {
					if (!NSR) synchronized (Mutex) {
						System.out.println("Entered Mutex Error");
						LOG.error(RSS_id+" ("+RSS_link+")");
						System.out.println("ERROR_XML:"+e.toString());
						PreparedStatement prepError = conn.prepareStatement("UPDATE RSS SET Syntax_error=?, Needs_syntax_recheck=1 WHERE RSS_id="+RSS_id+";");
						System.out.println("prepError");
						String Syntax_error="";
						Syntax_error+="ERROR_FEED:";
						Syntax_error+=e.toString();
						for (StackTraceElement STE : e.getStackTrace()){
							Syntax_error += "\nat "+STE.toString();
						}
						try {
							prepError.setBytes(1, Syntax_error.getBytes("UTF-8"));
						}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
						prepError.execute();
						System.out.println("Left Mutex Error");
						Mutex.notify();
					}
					return null;
				}
				else {
					synchronized (Mutex) {
						System.out.println("Entered Mutex Warn");
						LOG.warn("Warning: "+RSS_id+" ("+RSS_link+") : ERROR_XML:",e);
						System.out.println("ERROR_XML:"+e.toString());
						PreparedStatement prepError = conn.prepareStatement("UPDATE RSS SET Syntax_error=? WHERE RSS_id="+RSS_id+";");
						System.out.println("prepError");
						String Syntax_error="";
						Syntax_error+="ERROR_XML:";
						Syntax_error+=e.toString();
						for (StackTraceElement STE : e.getStackTrace()){
							Syntax_error += "\nat "+STE.toString();
						}
						try {
							prepError.setBytes(1, Syntax_error.getBytes("UTF-8"));
						}catch(UnsupportedEncodingException e1){LOG.info(RSS_id+"("+RSS_link+")");LOG.error("ERROR_ERROR:",e1);}
						prepError.execute();
						System.out.println("Left Mutex Warn");
						Mutex.notify();
					}
					message.add(msg);
					message.add(msg_bboff);
					return message;
				}
			}
		}catch(Exception e){LOG.error("ERROR_URL:",e);}

		//Добавляем в List сообщения с бб кодами и без бб кодов
		msg="";
		msg_bboff="";
		
		Collections.reverse(mesbbCol);
		Collections.reverse(mesCol);
		
		if (message.size()>0) {
			LOG.debug("List not empty");
			LOG.debug(message.toString());
		}
		
		message.clear();
		
		//BB
		for (String post : mesbbCol)
		{
			if (msg.length()+post.length()>=MAX_STANZAS) {
				message.add(msg);
				msg="";
			}
			msg+=post;
		}
		if (!msg.isEmpty()) {
			message.add(msg);
			msg="";
		}
		mesbbCol.clear();
		mesbbCol.addAll(message);
		message.clear();
		
		//noBB
		for (String post : mesCol)
		{
			if (msg_bboff.length()+post.length()>=MAX_STANZAS) {
				message.add(msg_bboff);
				msg_bboff="";
			}
			msg_bboff+=post;
		}
		if (!msg_bboff.isEmpty()) {
			message.add(msg_bboff);
			msg_bboff="";
		}
		mesCol.clear();
		mesCol.addAll(message);
		message.clear();

		//flushing
		message.add(mesbbCol.size()+" "+mesCol.size());
		message.addAll(mesbbCol);
		message.addAll(mesCol);
		//System.out.println(">>Message built<<");
		return message;
	}
	
	//Получаем последнюю запись прямо из БД
	public String getLast(String jid, String strid)
	{
		String message = "";//Получить бб-режим для пользователя
		Long Sub_id;
		try {Sub_id = new Long(strid);}catch(NumberFormatException e){LOG.info("Status: ERROR ID");message="Ошибка, проверьте правильность вводимых данных.";return message;}
		try {
			ResultSet rs;
			synchronized (Mutex) {
				//Проверяем просмотр своей подписки, для этого jid и id подписки должны быть равны
				rs = st.executeQuery("SELECT BB, RSS_last_content, RSS_lastbb_content FROM SUBS INNER JOIN USERS ON USERS.User_id=SUBS.User_id INNER JOIN RSS ON SUBS.RSS_id=RSS.RSS_id WHERE Sub_id="+Sub_id+" AND Jabber='"+jid+"';");
				if (rs.next()) {
					byte[] Content = rs.getBytes("RSS_last_content");
					byte[] Contentbb = rs.getBytes("RSS_lastbb_content");
					if (rs.wasNull()) {
						message="Нет записей.";
					}
					else {
						try {
							if (rs.getLong("BB")==0){
								message=new String(Content,"UTF-8");
							}
							else {
								message=new String(Contentbb,"UTF-8");
							}
						}catch(UnsupportedEncodingException e){LOG.error("ERROR_ERROR:",e);message="Нет записей.";return message;}
					}
				}
				else {
					message="Подписка не найдена.";
				}
			}
		}catch(Exception e){LOG.error("ERROR_SQL1:",e);}
		return message;
	}
	
	//Получаем список пользователей подписаных на указаную ленту без паузы и с указанным режимом ББ кодов 
	public List<String> listRSSUsers(long RSS_id, int BB, int conf)
	{
		List<String> rss_users = new ArrayList<String>();
		try 
		{
			//Получаем список пользователей, подписанных на этот RSS, без паузы и с указанным режимом BB
			synchronized (Mutex)
			{
				ResultSet rs = st.executeQuery("SELECT Jabber FROM SUBS INNER JOIN USERS ON USERS.User_id=SUBS.User_id WHERE RSS_id="+RSS_id+" AND Sub_pause=0 AND Is_conf="+conf+" AND BB="+BB+";");
				while(rs.next())
				{
					rss_users.add(rs.getString("jabber"));
				}
				rs.close();		
				Mutex.notify();
			}	
		}catch(Exception e){LOG.error("ERROR_URL:",e);}
		return rss_users;
	}
	
	//Устанавливаем/отключаем паузу для всех подписок 
	public String setPause(String jid, int pause)
	{
		String message = "";
		try 
		{
			//Используя всю силу SQL тупо ставим паузы на все подписки этого жида :D
			synchronized (Mutex)
			{
				st.executeUpdate("UPDATE SUBS SET Sub_pause="+pause+" WHERE User_id=(SELECT User_id FROM USERS WHERE Jabber='"+jid+"');");
				Mutex.notify();
			}
			LOG.info("Общая пауза: "+jid);
			if(pause!=0){message="Общая пауза для всех подписок включена.";}
			else{message="Общая пауза для всех подписок выключена.";}			
		}catch(Exception e){LOG.error("ERROR_PAUSE:",e);message="Ошибка включения/выключения паузы.";}
		return message;
	}
	//Отключение/включение бб кодов
	public String BBcode(String jid, int bbmode)
	{
		String message = "";
		try 
		{
			//Используя новую структуру меняем одно быдло-значение. Привет быдлокоду Благина!
			synchronized (Mutex)
			{
				st.executeUpdate("UPDATE USERS SET BB="+bbmode+" WHERE Jabber='"+jid+"';");
				Mutex.notify();
			}
			LOG.info("Отключение/включение бб кодов: "+jid);
			if(bbmode!=0){message="BB коды включены.";}
			else{message="BB коды выключены.";}			
		}catch(Exception e){LOG.error("ERROR_PAUSE:",e);message="Ошибка включения/выключения BB кодов.";}
		return message;
	}

	//Считаем ленты с паузой и без
	public String countUserRSS(String jid){
		String msg = "";
		ResultSet rs;
		try 
		{
			synchronized (Mutex) {
				rs = st.executeQuery("SELECT COUNT(*) FROM SUBS INNER JOIN USERS ON SUBS.User_id=USERS.User_id WHERE Jabber='"+jid+"' AND Sub_pause=0;");
				msg+="("+rs.getInt(1);
				//rs.close();
				rs = st.executeQuery("SELECT COUNT(*) FROM SUBS INNER JOIN USERS ON SUBS.User_id=USERS.User_id WHERE Jabber='"+jid+"';");
				msg+="/"+rs.getInt(1)+")";
				rs.close();
				Mutex.notify();
			}			
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
		return msg;
	}
	
	//Получаем ленты на которые подписан пользователь
	public String listUserRSS(String jid, int bbmode)
	{
		String msg = "";
		ResultSet rs;
		try 
		{
			//SELECT Sub_id, RSS_link, Sub_pause FROM RSS INNER JOIN SUBS ON SUBS.RSS_id=RSS.RSS_id INNER JOIN USERS ON USERS.User_id=SUBS.User_id WHERE Jabber='commaster@qip.ru';
			if (bbmode==0)
			{
				msg += "Ваши каналы (Your feeds): ";
				synchronized (Mutex)
				{
					rs = st.executeQuery("SELECT BB FROM USERS WHERE Jabber='"+jid+"';");
					if (rs.next())
					{
						if (rs.getLong("BB")==0)
						{
							msg += "(BB-коды выключены)";
						}
						else if (rs.getLong("BB")==1)
						{
							msg += "(BB-коды включены)";
						}
						else
						{
							msg += "(BB-коды неадекватны, сообщите разработчику)";
						}
					}
					else
					{
						msg += "(BB-коды неадекватны, сообщите разработчику)";
					}
					rs.close();
					msg += "\n";
					Mutex.notify();
				}
			}
			else
			{
				msg += "[table][tr][th width=30]ID[/th][th]Адрес/Address[/th][th width=90]Пауза/Pause[/th][/tr]";
			}
			
			synchronized (Mutex)
			{
				rs = st.executeQuery("SELECT Sub_id, RSS_link, Sub_pause FROM RSS INNER JOIN SUBS ON SUBS.RSS_id=RSS.RSS_id INNER JOIN USERS ON USERS.User_id=SUBS.User_id WHERE Jabber='"+jid+"';");
				if (bbmode==0)
				{
					msg += "ID - Адрес RSS - Статус\n";
				}
				while(rs.next())
				{
					if (bbmode==0)
					{
						msg += rs.getLong("Sub_id")+" "+rs.getString("RSS_link")+" - ";
						if (rs.getLong("Sub_pause")==0) {msg += "доставка";} else {msg += "приостановлено";}
						msg += "\n";
					}
					else
					{
						msg += "[tr][td][center]"+rs.getLong("Sub_id")+"[/center][/td][td]"+rs.getString("RSS_link")+"[/td][td][center]";
						if (rs.getLong("Sub_pause")==0) {msg += "Выкл/OFF";} else {msg += "Вкл/ON";}
						msg += "[/center][/td][/tr]";
					}
				}
				if (bbmode==1)
				{
					msg += "[/table]";
				}
				rs.close();
				Mutex.notify();
			}			
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
		return msg;
	}
	
	//Сохраняем RSS ленту, подписываем
	public int addSub(String RSS_link, String jid)
	{
		LOG.info("---------------------------------------------");
		LOG.info("Subscribe: "+jid+" feed:[ "+RSS_link+" ]");
		int i = 0;
		try 
		{
			new URL(RSS_link);
			try 
			{
				long RSS_id=0;
				ResultSet rs;
				synchronized (Mutex)
				{
					rs = st.executeQuery("SELECT RSS_id FROM RSS WHERE RSS_link='"+RSS_link+"'");//Узнаем ключ RSS
					if(!rs.next())//Лента новая, сохраняем.
					{
						LOG.info("New feed");
						st.executeUpdate("INSERT INTO RSS (RSS_link) VALUES ('"+RSS_link+"');"/*,Statement.RETURN_GENERATED_KEYS*/);
						LOG.info("New feed is up");
						rs = st.executeQuery("SELECT RSS_id FROM RSS WHERE RSS_link='"+RSS_link+"'");//Узнаем ключ RSS
						RSS_id=rs.getLong("RSS_id");
						i = 1;
					}
					else
					{
						RSS_id=rs.getLong("RSS_id");
					}
					rs.close();
					//Лента уже есть в списке, делаем подписку
					if (i==0) {LOG.info("New user for feed: "+RSS_link);}
					//Проверить подписку
					rs = st.executeQuery("SELECT Sub_id FROM SUBS INNER JOIN USERS ON USERS.User_id=SUBS.User_id WHERE Jabber='"+jid+"' AND RSS_id="+RSS_id+";");
					if(!rs.next())
					{
						//Подписываем пользователя
						st.executeUpdate("INSERT INTO SUBS (User_id, RSS_id) SELECT User_id,"+RSS_id+" FROM USERS WHERE Jabber='"+jid+"';");
						i = 1;
					}
					else
					{
						i = 2;/*Уже подписан, повторно посылает*/
					}
					rs.close();
					Mutex.notify();
				}
			}catch(SQLException e){i=-1;LOG.error("ERROR_SQL:",e);}
		}catch(MalformedURLException e){i=-1;LOG.error("ERROR_URL:",e);}
		LOG.info("---------------------------------------------");
		return i;
	}
	
	//Удаление подписки пользователя + проверка, на пустые каналы без пользователей
	public int delSub(String jid, String strid)
	{
		LOG.info("---------------------------------------------");
		LOG.info("Deleting a subscription: "+jid+" RSS_id: "+strid);
		int i = 0;
		Long Sub_id;
		try {Sub_id = new Long(strid);}catch(NumberFormatException e){i=1;LOG.info("Status: ERROR ID");return i;}
		try
		{
			ResultSet rs;
			synchronized (Mutex)
			{
				//Проверяем, удаляет ли он именно свою подписку
				rs = st.executeQuery("SELECT Sub_id FROM SUBS INNER JOIN USERS ON USERS.User_id=SUBS.User_id WHERE Sub_id="+Sub_id+" AND Jabber='"+jid+"';");
				if(rs.next())
				{
					i=2;
					st.executeUpdate("DELETE FROM SUBS WHERE Sub_id="+Sub_id+";");			
				}
				else
				{i=1;}
				rs.close();
				Mutex.notify();
			}
			//Проверка на пустые подписки
			deleteEmpty();
		}catch(Exception e){LOG.error("ERROR_SQL:",e);}
		if(i==1){LOG.info("Status: ERROR ID");}
		if(i==2){LOG.info("Status: OK");}
		LOG.info("---------------------------------------------");
		return i;
	}
	
	//Устанавливаем паузу для конкретной ленты пользователя
	public String pauseSub(String jid, String strid)
	{
		String message = "";
		LOG.info("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||");
		LOG.info("PAUSE a feed: "+jid+" id: "+strid);
		Long Sub_id;
		try {Sub_id = new Long(strid);}catch(NumberFormatException e){LOG.info("Status: ERROR ID");return "Ошибка, проверьте правильность вводимых данных.";}
		try
		{
			ResultSet rs;
			synchronized (Mutex) 
			{
				//Проверяем паузу ставит он именно свою подписку, для этого jid и id подписки должны быть равны
				rs = st.executeQuery("SELECT SUBS.User_id, Sub_pause FROM SUBS INNER JOIN USERS ON USERS.User_id=SUBS.User_id WHERE Sub_id="+Sub_id+" AND Jabber='"+jid+"';");
				if(rs.next())
	            {
					if(rs.getLong("Sub_pause")==0)
					{
						st.executeUpdate("UPDATE SUBS SET Sub_pause=1 WHERE User_id="+rs.getLong("User_id")+";");
						message="Пауза для подписки "+Sub_id+" включена.";
					}else
					{
						st.executeUpdate("UPDATE SUBS SET Sub_pause=0 WHERE User_id="+rs.getLong("User_id")+";");
						message="Пауза для подписки "+Sub_id+" выключена.";
					}									
	            }
				else
				{message="Ошибка, проверьте правильность вводимых данных.";}		
				rs.close();
				Mutex.notify();
			}
		}catch(Exception e){LOG.error("ERROR_SQL1:",e);}
		LOG.info("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||");
		return message;
	}
	
	//Узнаем сообщение об ошибке
	public String getError(long RSS_id)
	{
		ResultSet rs;
		try
		{
			synchronized (Mutex)
			{
				rs = st.executeQuery("SELECT Syntax_error FROM RSS WHERE RSS_id="+RSS_id+";");
				if (rs.next())
				{
					rs.getBytes("Syntax_error");
					if (rs.wasNull()) {
						rs.close();
						Mutex.notify();
						return "No such brocken feed!";
					}
					String message = new String(rs.getBytes("Syntax_error"),"UTF-8");
					rs.close();
					Mutex.notify();
					return "[quote][nobb]"+message+"[/nobb][/quote]";
				}
				else
				{
					rs.close();
					Mutex.notify();
					return "No such brocken feed!";
				}
			}
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
		 catch(UnsupportedEncodingException e){LOG.error("ERROR_ERROR:",e);}
		return "DB error";
	}
	
	//Узнаем линк на ленту
	public String getLink(long RSS_id)
	{
		ResultSet rs;
		try
		{
			synchronized (Mutex)
			{
				rs = st.executeQuery("SELECT RSS_link FROM RSS WHERE RSS_id="+RSS_id+";");
				if (rs.next())
				{
					String message = rs.getString("RSS_link");
					rs.close();
					Mutex.notify();
					return "[quote][nobb]"+message+"[/nobb][/quote]";
				}
				else
				{
					rs.close();
					Mutex.notify();
					return "No such feed!";
				}
			}
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
		return "UNKNOWN";
	}

	//Админ починил ленту, размораживаем
	public void pardonRSS(long RSS_id)
	{
		try
		{
			synchronized (Mutex)
			{
				st.executeUpdate("UPDATE RSS SET Needs_syntax_recheck=0, Syntax_error=NULL WHERE RSS_id="+RSS_id+";");
				LOG.info("RSS "+RSS_id+" unlocked.");
			}
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
	}
	
	//Удаляем пустые подписки, каналы на которые ни кто не подписан
	public void deleteEmpty()
	{
		LOG.info("---------------------------------------------");
		LOG.info("Remove empty subscriptions START");
		ResultSet rs;
		for (long RSS_id : listRSSFeeds())//Получить id всех каналов из таблицы RSS_feeds
		{
			try
			{
				//Проверить, есть ли RSS_id в таблице SUBS. Если нету - пустая лента
				synchronized (Mutex)
				{
					rs = st.executeQuery("SELECT User_id,Needs_syntax_recheck FROM SUBS INNER JOIN RSS ON SUBS.RSS_id=RSS.RSS_id WHERE SUBS.RSS_id="+RSS_id+";");
					if(!rs.next())
		            {
						LOG.info("Empty feed: "+RSS_id);
						//Удалить запись из таблицы RSS
						st.executeUpdate("DELETE FROM RSS WHERE RSS_id="+RSS_id+" AND Needs_syntax_recheck=0;");
					}
					rs.close();
					Mutex.notify();
				}
			}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
        }
		LOG.info("Remove empty subscriptions END");
		LOG.info("---------------------------------------------");
	}
	
	//Поиск пользователя по jid
	public boolean isUser(String JID)
	{
		ResultSet rs;
		try
		{
			synchronized (Mutex)
			{
				rs = st.executeQuery("SELECT User_id FROM USERS WHERE Jabber='"+JID+"';");
				if (rs.next())
				{
					rs.close();
					Mutex.notify();
					return true;
				}
				else
				{
					rs.close();
					Mutex.notify();
					return false;
				}
			}
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
		return false;
	}
	
	//Добавление пользователя по jid
	public void addUser(String JID, int conf)
	{
		try
		{
			synchronized (Mutex)
			{
				st.executeUpdate("INSERT INTO USERS (Jabber, Is_conf) VALUES ('"+JID+"',"+conf+");");
				Mutex.notify();
			}
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
	}
	
	//Изменение группы пользователя
	public boolean setUGroup(String JID, String UGroup)
	{
		try
		{
			synchronized (Mutex)
			{
				st.executeUpdate("UPDATE USERS SET UGroup='"+UGroup+"' WHERE Jabber='"+JID+"';");
				Mutex.notify();
			}
		}catch(SQLException e){LOG.error("ERROR_SQL:",e); return false;}
		return true;
	}
	
	//Получение группы пользователя
	public String getUGroup(String JID)
	{
		ResultSet rs;
		try
		{
			synchronized (Mutex)
			{
				rs = st.executeQuery("SELECT UGroup FROM USERS WHERE Jabber='"+JID+"';");
				if (rs.next())
				{
					String message = rs.getString("UGroup");
					rs.close();
					Mutex.notify();
					return message;
				}
				else
				{
					rs.close();
					Mutex.notify();
					return "No such user!";
				}
			}
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
		return "UNKNOWN";
	}
	
	//Удаление пользователя и его подписок по jid
	public void remUser(String JID)
	{
		LOG.info("---------------------------------------------");
		LOG.info("Removing a user: "+JID);
		try {
			synchronized (Mutex) {
				st.executeUpdate("DELETE FROM SUBS WHERE User_id=(SELECT User_id FROM USERS WHERE Jabber='"+JID+"');");
				st.executeUpdate("DELETE FROM USERS WHERE Jabber='"+JID+"';");
				Mutex.notify();
			}
			deleteEmpty();
		}catch(Exception e){LOG.error("ERROR_SQL:",e);}
		LOG.info("---------------------------------------------");
		return;
	}
	
	public boolean isConf(String MJID) {
		ResultSet rs;
		try
		{
			synchronized (Mutex)
			{
				rs = st.executeQuery("SELECT Is_conf FROM USERS WHERE Jabber='"+MJID+"';");
				if (rs.next())
				{
					if (rs.getInt("Is_conf")==1) {
						rs.close();
						Mutex.notify();
						return true;
					}
					else {
						rs.close();
						Mutex.notify();
						return false;
					}
					
				}
				else
				{
					rs.close();
					Mutex.notify();
					return false;
				}
			}
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
		return false;
	}
	
	public boolean isCA(String MJID, String User) {
		ResultSet rs;
		try {
			synchronized (Mutex) {
				//LOG.debug("SELECT Entry_id FROM CONF INNER JOIN USERS ON CONF.User_id=USERS.User_id WHERE Jabber='"+MJID+"' AND User='"+User+"';");
				rs = st.executeQuery("SELECT Entry_id FROM CONF INNER JOIN USERS ON CONF.User_id=USERS.User_id WHERE Jabber='"+MJID+"' AND User='"+User+"';");
				if (rs.next()) {
					rs.close();
					Mutex.notify();
					return true;
				}
				else {
					rs.close();
					Mutex.notify();
					return false;
				}
			}
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
		return false;
	}
	
	public void addCA(String MJID, String User) {
		try {
			synchronized (Mutex) {
				LOG.info(MJID+" for "+User);
				//LOG.debug("INSERT INTO CONF(User_id,User) VALUES ((SELECT User_id FROM USERS WHERE Jabber='"+MJID+"'),'"+User+"');");
				st.executeUpdate("INSERT INTO CONF(User_id,User) VALUES ((SELECT User_id FROM USERS WHERE Jabber='"+MJID+"'),'"+User+"');");
				Mutex.notify();
			}
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
	}
	
	public void delCA(String MJID, String User) {
		try {
			synchronized (Mutex) {
				st.executeUpdate("DELETE FROM CONF WHERE User_id=(SELECT User_id FROM USERS WHERE Jabber='"+MJID+"') AND User='"+User+"';");
				Mutex.notify();
			}
		}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
	}
	
	public List<String> SQLQuery(String Query) {
		ResultSet rs;
		List<String> answer = new ArrayList<String>();
		synchronized (Mutex) {
			try {
				answer.add("Query result:");
				rs = st.executeQuery(Query);
				int columns = 0;
				while (rs.next()) {
					columns = rs.getMetaData().getColumnCount();
					String QLine="";
					for (int i=1;i<=columns;i++){
						//String ColumnType = rs.getMetaData().getColumnTypeName(i);
						QLine += rs.getString(i) + "|";
					}
					answer.add(QLine);
				}
				if (columns==0) {
					answer.add("||Empty.||");
				}
				rs.close();
			}catch(SQLException e){LOG.error("ERROR_SQL:",e); answer.add("||Error_SQL.||");}
			Mutex.notify();
		}
		return answer;
	}
	
	public boolean SQLUpdate(String Query) {
		boolean answer=false;
		synchronized (Mutex)
		{
			try
			{
				st.executeUpdate(Query);
				answer=true;
			}catch(SQLException e){LOG.error("ERROR_SQL:",e);}
			Mutex.notify();
		}
		return answer;
	}
	
	//Проверка работы БД
	public void ping()
	{
		try
		{
			synchronized (Mutex)
			{
				System.out.println("DB ping");
				Mutex.notify();
			}
		}catch(Exception e){LOG.error("ERROR_DBPING:",e);}
	}
	
}
/*******************************************************************************************************************/