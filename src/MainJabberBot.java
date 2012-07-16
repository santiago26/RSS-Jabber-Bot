import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
//import java.util.Random;
import java.lang.Math;
import java.io.*;

import org.jivesoftware.smack.*;
//import org.jivesoftware.smack.filter.AndFilter;
//import org.jivesoftware.smack.filter.PacketExtensionFilter;
import org.jivesoftware.smack.filter.PacketFilter;
import org.jivesoftware.smack.filter.PacketTypeFilter;
//import org.jivesoftware.smack.packet.Message.Body;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Packet;
import org.jivesoftware.smack.packet.Presence;

import org.jivesoftware.smackx.filetransfer.*;
//import org.jivesoftware.smackx.filetransfer.FileTransfer.*;
import org.jivesoftware.smackx.muc.*;

import org.apache.log4j.Logger;

import rss2bb.sec.*;

class JabberBot implements Runnable
{
	public static final Logger LOG=Logger.getLogger(JabberBot.class);
	private ConnectionConfiguration connConfig;
    private XMPPConnection connection;
    private boolean status = true;
    private boolean Stop_refresh = false;
    String Login = account.Login;
    String Password = account.Password;
    String Domain = account.Domain;
    String mucName = account.mucName;
    
    String help = "rssbot@qip.ru - XMPP(jabber) бот, рассылающий новостные RSS ленты, оформленные в BB-коды.\n" +
			"--------------------------------------------------------\n" +
			"Подписка на ленту новостей:\n" +
			"s rss_url [где rss_url, URL адрес RSS ленты, на которую вы хотите подписаться]\n" +
			"\n" +
			"Вывод списка лент, на которые вы подписаны и состояние паузы:\n" +
			"list\n" +
			"\n" +
			"Удаление подписки на ленту новостей:\n" +
			"d 123 [где 123, идентификатор вашей подписки, узнать который можно при помощи команды list]\n" +
			"\n" +
			"Включение/Отключение паузы на определенную ленту:\n" +
			"p 123 [где 123, идентификатор вашей подписки, узнать который можно при помощи команды list]\n" +
			"\n" +
			"Просмотр последнего обновления подписки:\n" +
			"l 123/last 123 [где 123, идентификатор вашей подписки, узнать который можно при помощи команды list]\n" +
			"\n" +
			"Включение общей паузы для всех подписок:\n" +
			"pause on\n" +
			"Отключение общей паузы для всех подписок:\n" +
			"pause off\n" +
			"\n" +
			"Включение BB кодов в сообщениях:\n" +
			"bb on\n" +
			"Отключение BB кодов в сообщениях:\n" +
			"bb off\n" +
			"\n" +
			"Приглашение в конференцию:\n" +
			"join room@domain.name [где room@domain.name - адрес конференции]\n" +
			"\n" +
			"Проверка связи с ботом:\n" +
			"ping\n" +
			"\n" +
			"Предложения, пожелания:\n" +
			"idea words [где words, текст предложения, пожелания]\n" +
			"Например: idea Давайте захватим мир!\n" +
			"\n" +
			"Вывод справки:\n" +
			"help/?/h\n" +
			"--------------------------------------------------------\n" +
			"Для того чтобы начать получать новостные ленты, достаточно добавить себе в контакт лист jid: rssbot@qip.ru и подписаться на RSS ленту и после этого вам будут приходить новости в виде сообщений оформленных в BB кодах.\n" +
			"\n" +
			"Предупреждение:\n" +
			"Версия бота находиться в стадии бета-тестирования, возможны проблемы с распознаванием html кода в теле RSS ленты, так же иные проблемы, просьба сообщить о замеченных недочетах по адресу jabber: Commaster@qip.ru\n" +
			"\n" +
			"Страница программы: http://qiptester.ru/rssbot";
    
    String sHelp="Приветствую тебя, дорогой админ!\n" +
            "\n" +
            "Я знаю одну фичу, о которой никто не знает:\n" +
            "listbb - отобразит твои подписки в гламурной бб-табличке.\n" +
            "\n" +
            "Ну а так, как ты админ, то я расскажу тебе про очень важные функции.\n" +
            "Но ты должен обещать, что сначала подумаешь, прежде чем вызвать любую из них.\n" +
            "\n" +
            "roster - покажет все контакты в моем КЛ.\n" +
            "users - покажет всех зарегистрированых пользователей.\n" +
            "close - выключит меня полностью.\n" +
            "restart - перезапустит меня, в надежде исправить какой-то глюк или загрузить новую сборку.\n" +
            "empty - пустит меня в плаванье по подпискам и лентам для нахождения лент без подписоки их уничтожения.\n" +
            "rev/revision - почешет твое ЧСВ и покажет номер моей сборки.\n" +
            "pingdb - потыкает БД палочкой.\n" +
            "listerrors - даст список всех забаненых лент.\n" +
            "listerrorsbb - даст подробный список всех забаненых лент.\n" +
            "geterror 123 - расскажет, что же за беда слечилась с лентой 123.\n" +
            "pardon 123 - даст ленте 123 еще один шанс.\n" +
            "getlink 123 - покажет истинный источник ленты 123.\n" +
            "slast name@domain 123/sl name@domain 123 - покажет последние записи в подписке от имени данного jid-а.\n" +
            "adduser name@domain - добавит данный jid в базу, но не отправит ему help.\n" +
            "remuser name@domain - удалит данный jid из базы вместе со всеми его подписками.\n" +
            "slist name@domain - расскажет тебе про подписки данного jid-а.\n" +
            "say name@domain - отправит твое сообщение данному jid-у.\n" +
            "csay name@domain - отправит твое сообщение данному jid-у до-словно.\n" +
            "rehead - обновит все ленты, не рассылая сообщений.\n" +
            "getlog - перешлет тебе последний метр лога.\n" +
            "SQLQuery Query - ТОЛЬКО для SELECT запросов.\n" +
            "SQLUpdate Query - для запросов UPDATE и DELETE.\n" +
            "\n" +
            "И теперь, когда ты знаешь все. ... Не сломай мир :)";
    
	@Override
	public void run()
	{
		LOG.info("Run JabberBot thread...");
		//Connection.DEBUG_ENABLED=true;
		connConfig = new ConnectionConfiguration("webim.qip.ru",5222,Domain);
		SASLAuthentication.supportSASLMechanism("PLAIN");
		connConfig.setCompressionEnabled(false);
    	connConfig.setSASLAuthenticationEnabled(true);
    	connConfig.setReconnectionAllowed(true);
    	connConfig.setRosterLoadedAtLogin(true);
    	connection = new XMPPConnection(connConfig);
    	final database db = new database();
        try
        {
        	//LOG.info("Connecting...");
        	long connectionattempt=1;
        	{
        		while (!connection.isConnected()){
            		try {
    					connection.connect();
    				} catch(Exception e){
    					connectionattempt++;
    					//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            			try{Thread.sleep(60000);}catch(Exception e1){LOG.error("ERROR_THREAD:",e1);}
            			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    				}
            	}
        	}
        	LOG.info("Connected on "+connectionattempt+" try.");
        	//LOG.info("Logging in...");
            connection.login(Login,Password,"rssbot");
            //LOG.info("Logged in...");
            Presence presence = new Presence(Presence.Type.available);
            presence.setStatus("RSS Jabber Bot: активен");
            presence.setPriority(50);
            connection.sendPacket(presence);
            //LOG.info("Presence sent...");
            
            PacketFilter pfMain = new PacketTypeFilter(Message.class);
            final PacketListener plMain = new PacketListener() {
                public void processPacket(Packet packet) 
                {
                	System.out.println("Got Packet");
                	if(packet instanceof Message)
                    {
                        Message message    = (Message) packet;
                        switch (message.getType()) {
                        	case chat:{
                        		System.out.println("Got Message");
                        		String messageBody = "";
                        		{
                        			Collection<Message.Body> bodies = message.getBodies();
                        			for(Message.Body r:bodies){messageBody += r.getMessage();}
                        		}
                        		String JID         = message.getFrom();
                        		//conference block module
                        		if (JID.contains("conference"))
                        		{
                        			System.out.println("Conference blocked");
                        			return;
                        		}
                        		boolean MessageProcessed = false;
                        		if (messageBody.length()==0){
                        			System.out.println("Empty message");
                        			return;
                        		}
                        		LOG.info("---------------------------------------------[MESSAGE START]");
                        		LOG.info(JID+": "+messageBody);
                        		LOG.info("---------------------------------------------[MESSAGE END]");

                        		String messageBodyO=messageBody;
                        		
                        		String Resource = JID.replaceAll("^(.*?)[/](.*+)$","$2");
                        		JID = JID.replaceAll("^(.*?)[/](.*+)$","$1").toLowerCase();

                        		messageBody = messageBody.trim();
                        		String command = messageBody.replaceAll("^(.*)[ ](.*+)$","$1");
                        		if(messageBody.indexOf(" ")!=-1){messageBody = messageBody.replaceAll("^(.*)[ ](.*+)$"," $2");}else{messageBody="";}
                        		messageBody = (command.toLowerCase().trim()+messageBody);

                        		String Command;
                        		if (messageBody.indexOf(" ")==-1)
                        		{
                        			Command = messageBody;
                        		}
                        		else
                        		{
                        			Command = messageBody.substring(0, messageBody.indexOf(" "));
                        		}

                        		//command only for admin
                        		if(JID.equals("commaster@qip.ru")||JID.equals("santiago26@qip.ru"))
                        		{
                        			LOG.info("Is admin");
                        			switch (Command)
                        			{
                        			case "roster":{
                        				getRoster(JID);
                        				MessageProcessed = true;
                        			}break;
                        			case "users":{
                        				String Message = "";
                        				List<String> Lfoo;
                        				Lfoo=db.listUsers();
                        				Message += "Пользователи("+Lfoo.size()+"):";
                        				for (String LJID : Lfoo) {
                        					Message += "\t";
                        					Message += LJID;
                        					Message += db.countUserRSS(LJID);
                        				}
                        				Lfoo=db.listConf();
                        				Message += ".\nКонференции("+Lfoo.size()+"):";
                        				for (String MJID : Lfoo) {
                        					Message += "\t";
                        					Message += MJID;
                        					Message += db.countUserRSS(MJID);
                        				}
                        				Message += ".";
                        				sendMessage(JID, Message);
                        				MessageProcessed = true;
                        			}break;
                        			case "listerrors":{
                        				String Message = "";
                        				List<Integer> Lfoo;
                        				Lfoo=db.listErrors();
                        				Message += "Ошибки("+Lfoo.size()+"): ";
                        				for (Integer ID : Lfoo) {
                        					if (!ID.equals(Lfoo.get(0))) Message += ", ";
                        					Message += ID;
                        				}
                        				Message += ".";
                        				sendMessage(JID, Message);
                        				MessageProcessed = true;
                        			}break;
                        			case "listerrorsbb":{
                        				String Message = "";
                        				List<Integer> Lfoo;
                        				Lfoo=db.listErrors();
                        				Message += "[table][tr][th width=30]ID[/th][th]Ошибка[/th][/tr]";
                        				for (Integer ID : Lfoo) {
                        					//LOG.info("Reading ID="+ID);
                        					Message += "[tr][td]" +ID+ "[/td][td]" +db.getError(ID)+ "[/td][/tr]";
                        				}
                        				Message += "[/table]";
                        				sendMessage(JID, Message);
                        				MessageProcessed = true;
                        			}break;
                        			case "close":{
                        				connection.disconnect();
                        				MessageProcessed = true;
                        				status = false;
                        				LOG.info("Выход из программы!");
                        				System.exit(0);
                        			}break;
                        			case "restart":{
                        				connection.disconnect();
                        				MessageProcessed = true;
                        				status = false;
                        				LOG.info("Перезапуск программы!");
                        				restartApplication();
                        			}break;
                        			case "empty":{
                        				db.deleteEmpty();
                        				MessageProcessed = true;
                        			}break;
                        			case "rev":case "revision":{
                        				getRevision(JID);
                        				MessageProcessed = true;
                        			}break;
                        			case "pingdb":{
                        				sendMessage(JID,"Queueing DB ping action...");
                        				db.ping();
                        				sendMessage(JID,"DB says pong!");
                        				MessageProcessed = true;
                        			}break;
                        			case "geterror":{
                        				long RSS_id = Long.parseLong(messageBody.substring(messageBody.indexOf(" ")+1));
                        				sendMessage(JID,db.getError(RSS_id));
                        				MessageProcessed = true;
                        			}break;
                        			case "pardon":{
                        				String param = messageBody.substring(messageBody.indexOf(" ")+1);
                        				if (param.equals("all")) {
                        					List<Integer> Lfoo;
                        					Lfoo=db.listErrors();
                        					for (Integer ID : Lfoo) {
                        						db.pardonRSS(ID);
                        					}
                        					sendMessage(JID,"All feeds unlocked.");
                        				}
                        				else {
                        					long RSS_id = Long.parseLong(param);
                        					db.pardonRSS(RSS_id);
                        					sendMessage(JID,"RSS "+RSS_id+" unlocked.");
                        				}
                        				MessageProcessed = true;
                        			}break;
                        			case "getlink":{
                        				long RSS_id = Long.parseLong(messageBody.substring(messageBody.indexOf(" ")+1));
                        				sendMessage(JID,db.getLink(RSS_id));
                        				MessageProcessed = true;
                        			}break;
                        			case "adduser":{
                        				String AJID = messageBody.substring(messageBody.indexOf(" ")+1);
                        				db.addUser(AJID,0);
                        				sendMessage(JID,"User "+AJID+" added!");
                        				MessageProcessed=true;
                        			}break;
                        			case "remuser":{
                        				String DJID = messageBody.substring(messageBody.indexOf(" ")+1);
                        				db.remUser(DJID);
                        				sendMessage(JID,"User "+DJID+" removed!");
                        				MessageProcessed=true;
                        			}break;
                        			case "slist":{
                        				String LJID = messageBody.substring(messageBody.indexOf(" ")+1);
                        				sendMessage(JID,db.listUserRSS(LJID, 1));
                        				MessageProcessed=true;
                        			}break;
                        			case "slast":{
                        				String LJID = messageBody.substring(messageBody.indexOf(" ")+1);
                        				String strid = LJID.substring(LJID.indexOf(" ")+1);
                        				LJID = LJID.substring(0,LJID.indexOf(" "));
                        				sendMessage(JID,db.getLast(LJID,strid));
                        				MessageProcessed = true;
                        			}break;
                        			case "sl":{
                        				String LJID = messageBody.substring(messageBody.indexOf(" ")+1);
                        				String strid = LJID.substring(LJID.indexOf(" ")+1);
                        				LJID = LJID.substring(0,LJID.indexOf(" "));
                        				sendMessage(JID,db.getLast(LJID,strid));
                        				MessageProcessed = true;
                        			}break;
                        			case "rehead":{
                        				sendMessage(JID,"Reheading initialized!");
                        				Stop_refresh=true;
                        				for (Long RSS_id : db.getRSSFeeds())
                        				{
                        					//Получаем одну RSS ленту

                        					//Проверяем и получаем новые записи для этой ленты
                        					List<String> data = db.getNew(RSS_id, true);

                        					if (data==null)
                        					{
                        						//sendMessage("santiago26@qip.ru",RSS_id.toString()+" haz problems.");
                        						//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        						//try{Thread.sleep(1000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
                        						//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        						sendMessage("commaster@qip.ru",RSS_id.toString()+" haz problems.");
                        						//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        						try{Thread.sleep(1000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
                        						//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        						continue;
                        					}
                        				}
                        				Stop_refresh=false;
                        				sendMessage(JID,"Reheading complete!");
                        				MessageProcessed = true;
                        			}break;
                        			case "say":{
                        				String TJID = messageBodyO.substring(messageBodyO.indexOf(" ")+1);
                        				String Text = TJID.substring(TJID.indexOf(" ")+1);
                        				TJID = TJID.substring(0,TJID.indexOf(" "));
                        				sendMessage(TJID,"Из пространства внезапно донеслось: "+Text);
                        				//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        				try{Thread.sleep(1000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
                        				//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        				sendMessage(JID,"Отправлено...");
                        				MessageProcessed = true;
                        			}break;
                        			case "csay":{
                        				String TJID = messageBodyO.substring(messageBodyO.indexOf(" ")+1);
                        				String Text = TJID.substring(TJID.indexOf(" ")+1);
                        				TJID = TJID.substring(0,TJID.indexOf(" "));
                        				sendMessage(TJID,Text);
                        				//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        				try{Thread.sleep(1000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
                        				//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        				sendMessage(JID,"Отправлено...");
                        				MessageProcessed = true;
                        			}break;
                        			case "sqlquery":{
                        				String Query = messageBodyO.substring(messageBodyO.indexOf(" ")+1);
                        				List<String> rs = db.SQLQuery(Query);
                        				String Message="";
                        				for (String RSLine : rs) {
                        					Message+=RSLine + "\n";
                        				}
                        				sendMessage(JID,Message);
                        				MessageProcessed = true;
                        			}break;
                        			case "sqlupdate":{
                        				String Query = messageBodyO.substring(messageBodyO.indexOf(" ")+1);
                        				boolean complete = db.SQLUpdate(Query);
                        				if (complete) sendMessage(JID,"Success");
                        				else sendMessage(JID,"Failure");
                        				MessageProcessed = true;
                        			}break;
                        			case "shelp":{
                        				sendMessage(JID,sHelp);
                        				MessageProcessed = true;
                        			}break;
                        			case "getlog":{
                        				//LOG.info("Starting file transfer");
                        				FileTransferManager logTManager = new FileTransferManager(connection);
                        				OutgoingFileTransfer logTransfer = logTManager.createOutgoingFileTransfer(JID+"/"+Resource);
                        				try {
											logTransfer.sendFile(new File("log.cpp"), "Latest log file");
											while (!logTransfer.isDone()){}
										}catch (XMPPException e){sendMessage(JID,"Передача не удалась");}
                        				sendMessage(JID,"Передача завершена");
                        				MessageProcessed = true;
                        			}break;
                        			}
                        		}
                        		//common commands for all
                        		switch(messageBody)
                        		{
                        		case "ping":sendMessage(JID,"pong");MessageProcessed = true;break;
                        		case "list":sendMessage(JID,db.listUserRSS(JID,0));MessageProcessed = true;break;
                        		case "listbb":sendMessage(JID,db.listUserRSS(JID,1));MessageProcessed = true;break;
                        		case "pause on":sendMessage(JID,db.setPause(JID,1));MessageProcessed = true;break;//Общая пауза включить
                        		case "pause off":sendMessage(JID,db.setPause(JID,0));MessageProcessed = true;break;//Общая пауза выключить
                        		case "bb on":sendMessage(JID,db.BBcode(JID,1));MessageProcessed = true;break;//Включить ВВ коды
                        		case "bb off":sendMessage(JID,db.BBcode(JID,0));MessageProcessed = true;break;//Отключить ВВ коды
                        		case "help":case "?":case "h":sendMessage(JID,help);MessageProcessed = true;break;//Вывод справки
                        		default:
                        		{
                        			if (messageBody.length()<3) break;
                        			switch(messageBody.substring(0,2).toLowerCase())
                        			{
                        			case "s ":
                        			{
                        				String rss_url = messageBody.substring(2).trim();
                        				int i = db.addSub(rss_url,JID);
                        				if(i == 1){sendMessage(JID,"Вы подписаны (You are subscribed).");}
                        				if(i == 2){sendMessage(JID,"Вы уже подписаны на данную ленту новостей (You are already subscribed to this feed).");}
                        				MessageProcessed = true;
                        			}break;
                        			case "d ":
                        			{
                        				String strid = messageBody.substring(2).trim();
                        				int i = db.delSub(JID,strid);
                        				if(i==1){sendMessage(JID,"Ошибка номера подписки, наберите команду [i]list[/i] (Error number of subscription, type in the command [i]list[/i]).");}
                        				if(i==2){sendMessage(JID,"Ваша подписка удалена (Your subscription is removed).");}
                        				MessageProcessed = true;
                        			}break;
                        			case "p ":
                        			{
                        				String strid = messageBody.substring(2).trim();
                        				sendMessage(JID,db.pauseSub(JID,strid));
                        				MessageProcessed = true;
                        			}break;
                        			case "l ":
                        			{
                        				String strid = messageBody.substring(2).trim();
                        				sendMessage(JID,db.getLast(JID,strid));
                        				MessageProcessed = true;
                        			}break;
                        			default:
                        			{
                        				if (messageBody.length()<6) break;
                        				switch(messageBody.substring(0,5).toLowerCase())
                        				{
                        				case "idea ":
                        				{
                        					String idea = messageBody.substring(5).trim();
                        					String FromJID = JID;
                        					sendMessage("santiago26@qip.ru",FromJID+" предложил такую идею: "+idea);
                        					sendMessage(FromJID,"Спасибо за твои чудесные идеи, о великий юзер!");
                        					MessageProcessed = true;
                        				}break;
                        				case "last ":
                        				{
                        					String strid = messageBody.substring(5).trim();
                        					sendMessage(JID,db.getLast(JID,strid));
                        					MessageProcessed = true;
                        				}break;
                        				case "join ":
                        				{
                        					String MJID = messageBody.substring(5).trim();
                        					LOG.info("+++++++++++++++++++++++++++++++++++++++++++++[SYSTEM START]");
                        					LOG.info(MJID+" private invitation from "+JID);
                        					LOG.info("+++++++++++++++++++++++++++++++++++++++++++++[SYSTEM END]");

                        					MultiUserChat muc = new MultiUserChat(connection,MJID);
                        					if (muc.isJoined()) return;
                        					DiscussionHistory history = new DiscussionHistory();
                        					history.setMaxChars(0);
                        					try {
                        						muc.join(mucName, "", history, SmackConfiguration.getPacketReplyTimeout());
                        						if (!muc.isJoined()) return;
                        						muc.sendMessage("Мда-да?/Wazzzup?");
                        						//Issuer=muc.getOccupant(Issuer).getJid().replaceAll("(.*?)[/].*","$1").toLowerCase();
                        						System.out.println("plMUC for " + MJID);
                        						if (!db.isUser(MJID)) {
                        							db.addUser(MJID, 1);
                        						}
                        						if (!db.isCA(MJID,JID)) {
                        							db.addCA(MJID,JID);
                        						}
                        						sendMessage(JID,"Присоеденились!");
                        					}catch(XMPPException e){LOG.error("ERROR_MUC("+MJID+"):",e);sendMessage(JID,"Не удалось присоедениться. Проверьте настройки комнаты и попробуйте еще раз.");}
                        					MessageProcessed = true;
                        				}break;
                        				default:/*sendMessage(JID,"Unknown command");*/break;
                        				}
                        			}
                        			}//end switch

                        		}break;
                        		}//end switch
                        		if (!MessageProcessed)
                        		{
                        			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        			try{Thread.sleep(3000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
                        			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        			String reply="";
                        			reply += messageBodyO.substring(0, Math.min(100, messageBodyO.length()));
                        			if (messageBodyO.length()>100)
                        			{
                        				reply += "...";
                        			}
                        			reply += ": нет такой команды\n";
                        			reply += messageBodyO.substring(0, Math.min(100, messageBodyO.length()));
                        			if (messageBodyO.length()>100)
                        			{
                        				reply += "...";
                        			}
                        			reply += ": no such command\n";

                        			String FromJID = JID;
                        			LOG.info("default to " + FromJID);
                        			sendMessage(FromJID,reply);                        	
                        		}
                        	}break;
                        	case groupchat:{
                        		System.out.println("Got MUCMessage");
                        		if (message.getBody().startsWith(mucName+": "))
                        		{
                        			System.out.println("My Message");
                        			String MUCJID = message.getFrom();
                        			System.out.println("From "+MUCJID);
                        			String messageBody = message.getBody().substring(mucName.length()+2);
                        			System.out.println("Which says:"+messageBody);
                        			String MUC = MUCJID.replaceAll("^(.*?)[/](.*+)$","$1").toLowerCase();
                        			System.out.println("Chat is:"+MUC);
                        			String MUser = MUCJID.replaceAll("^(.*?)[/](.*+)$","$2");
                        			System.out.println("Author is:"+MUser);
                        			//if (!db.isCA(MUC,MUser)) return;
                        			@SuppressWarnings("unused")
									boolean MessageProcessed = false;
                        			if (messageBody.length()==0) return;
                        			LOG.info("---------------------------------------------[MESSAGE START]");
                        			LOG.info(MUC+"/"+MUser+": "+messageBody);
                        			LOG.info("---------------------------------------------[MESSAGE END]");

                        			//String messageBodyO=messageBody;

                        			messageBody = messageBody.trim();
                        			String command = messageBody.replaceAll("^(.*)[ ](.*+)$","$1");
                        			if(messageBody.indexOf(" ")!=-1){messageBody = messageBody.replaceAll("^(.*)[ ](.*+)$"," $2");}else{messageBody="";}
                        			messageBody = (command.toLowerCase().trim()+messageBody);

                        			//common commands for all
                        			switch(messageBody) {
                        				case "ping":sendMUCMessage(MUC,MUser,"pong");MessageProcessed = true;break;
                        				case "list":sendMUCMessage(MUC,MUser,db.listUserRSS(MUC,0));MessageProcessed = true;break;
                        				case "listbb":sendMUCMessage(MUC,MUser,db.listUserRSS(MUC,1));MessageProcessed = true;break;
                        				case "pause on":sendMUCMessage(MUC,MUser,db.setPause(MUC,1));MessageProcessed = true;break;//Общая пауза включить
                        				case "pause off":sendMUCMessage(MUC,MUser,db.setPause(MUC,0));MessageProcessed = true;break;//Общая пауза выключить
                        				case "bb on":sendMUCMessage(MUC,MUser,db.BBcode(MUC,1));MessageProcessed = true;break;//Включить ВВ коды
                        				case "bb off":sendMUCMessage(MUC,MUser,db.BBcode(MUC,0));MessageProcessed = true;break;//Отключить ВВ коды
                        				case "help":case "?":case "h":sendMUCMessage(MUC,MUser,help);MessageProcessed = true;break;//Вывод справки
                        				case "leave":{
                        					sendMUCBroadcast(MUC,"И не звоните мне больше!");
                        					leaveMUC(MUC);
                        					db.remUser(MUC);
                        					MessageProcessed=true;
                        				}break;
                        				default: {
                        					if (messageBody.length()<3) break;
                        					switch(messageBody.substring(0,2).toLowerCase()) {
                        						case "s ":{
                        							String rss_url = messageBody.substring(2).trim();
                        							int i = db.addSub(rss_url,MUC);
                        							if(i == 1){sendMUCMessage(MUC,MUser,"Вы подписаны (You are subscribed).");}
                        							if(i == 2){sendMUCMessage(MUC,MUser,"Вы уже подписаны на данную ленту новостей (You are already subscribed to this feed).");}
                        							MessageProcessed = true;
                        						}break;
                        						case "d ":{
                        							String strid = messageBody.substring(2).trim();
                        							int i = db.delSub(MUC,strid);
                        							if(i==1){sendMUCMessage(MUC,MUser,"Ошибка номера подписки, наберите команду [i]list[/i] (Error number of subscription, type in the command [i]list[/i]).");}
                        							if(i==2){sendMUCMessage(MUC,MUser,"Ваша подписка удалена (Your subscription is removed).");}
                        							MessageProcessed = true;
                        						}break;
                        						case "p ":{
                        							String strid = messageBody.substring(2).trim();
                        							sendMUCMessage(MUC,MUser,db.pauseSub(MUC,strid));
                        							MessageProcessed = true;
                        						}break;
                        						case "l ":{
                        							String strid = messageBody.substring(2).trim();
                        							sendMUCMessage(MUC,MUser,db.getLast(MUC,strid));
                        							MessageProcessed = true;
                        						}break;
                        						default:{
                        							if (messageBody.length()<6) break;
                        							switch(messageBody.substring(0,5).toLowerCase()) {
                        								case "idea ":{
                        									String idea = messageBody.substring(5).trim();
                        									String FromJID = MUC+"/"+MUser;
                        									sendMessage("santiago26@qip.ru",FromJID+" предложил такую идею: "+idea);
                        									sendMUCMessage(MUC,MUser,"Спасибо за твои чудесные идеи, о великий юзер!");
                        									MessageProcessed = true;
                        								}break;
                        								case "last ":
                        								{
                        									String strid = messageBody.substring(5).trim();
                        									sendMUCMessage(MUC,MUser,db.getLast(MUC,strid));
                        									MessageProcessed = true;
                        								}break;
                        								default:/*sendMessage(JID,"Unknown command");*/break;
                        							}
                        						}
                        					}//end switch
                        				}break;
                        			}//end switch
                        			return;
                        		}
                        	}break;
                        	default:{
                        		return;
                        	}
                        }
                    }
                }
            };
            connection.addPacketListener(plMain, pfMain);
            
            MultiUserChat.addInvitationListener(connection, new InvitationListener(){
				public void invitationReceived(Connection conn, String MJID,
						String Issuer, String reason, String password,
						Message message) {
					LOG.info("+++++++++++++++++++++++++++++++++++++++++++++[SYSTEM START]");
                    LOG.info(MJID+" invitation from "+Issuer);
                    LOG.info("+++++++++++++++++++++++++++++++++++++++++++++[SYSTEM END]");
                    
                    MultiUserChat muc = new MultiUserChat(connection,MJID);
                    if (muc.isJoined()) return;
                    DiscussionHistory history = new DiscussionHistory();
                    history.setMaxChars(0);
                    try {
						muc.join(mucName, "", history, SmackConfiguration.getPacketReplyTimeout());
						if (!muc.isJoined()) return;
						muc.sendMessage("Мда-да?/Wazzzup?");
						//Issuer=muc.getOccupant(Issuer).getJid().replaceAll("(.*?)[/].*","$1").toLowerCase();
						System.out.println("plMUC for " + MJID);
						if (!db.isUser(MJID)) {
							db.addUser(MJID, 1);
						}
						if (!db.isCA(MJID,Issuer)) {
							db.addCA(MJID,Issuer);
						}
						sendMessage(Issuer,"Присоеденились!");
					}catch(XMPPException e){LOG.error("ERROR_MUC("+MJID+"):",e);sendMessage(Issuer,"Не удалось присоедениться. Проверьте настройки комнаты и попробуйте еще раз.");}
				}
            });
            
            Roster.setDefaultSubscriptionMode(Roster.SubscriptionMode.accept_all);
            Roster roster = connection.getRoster();
            roster.setSubscriptionMode(Roster.SubscriptionMode.accept_all);
            roster.addRosterListener(new RosterListener() 
            {
            	public void entriesAdded(Collection<String> addresses) 
				{
            		for (String new_user : addresses) {
            			if (db.isUser(new_user)) addresses.remove(new_user);
            		}
            		if (addresses.size()==0) return;
            		LOG.info("//////////////////////////////////////////////////////////");
            		LOG.info("Added a new account: "+addresses);
            		LOG.info("//////////////////////////////////////////////////////////");
            		for (String new_user : addresses) {
            			sendMessage(new_user, help);
            			db.addUser(new_user,0);
            		}
				}
				public void entriesDeleted(Collection<String> addresses){}
                public void entriesUpdated(Collection<String> addresses){}
                public void presenceChanged(Presence presence){/*System.out.println("Presence changed: " + presence.getFrom() + " " + presence);*/}
            });
            //LOG.info("Listeners up...");
            
            //Заходим в конференции
            for (String MJID : db.listConf()) {
            	MultiUserChat muc = new MultiUserChat(connection,MJID);
                if (muc.isJoined()) break;
                DiscussionHistory history = new DiscussionHistory();
                history.setMaxChars(0);
                try {
					muc.join(mucName, "", history, SmackConfiguration.getPacketReplyTimeout());
					//muc.sendMessage("Скучали?/Have you missed me?");
					//Issuer=muc.getOccupant(Issuer).getJid().replaceAll("(.*?)[/].*","$1").toLowerCase();
					System.out.println("plMUC for " + MJID);
				}catch(XMPPException e){LOG.error("ERROR_MUC("+MJID+"):",e);}
            }
            
            while(status)
    		{
            	//LOG.info("----------------[ While status true ]----------------");
            	if((connection.isConnected())&&(!Stop_refresh))
            	{
            		//LOG.info("----------------[ Connection is connected ]----------------");
            		try 
        			{
        				//Получаем все RSS каналы из базы данных
        				//database db = new database();
        				LOG.info("Getting RSSFeeds...");
        				for (Long RSS_id : db.getRSSFeeds())
        				{
        					//Получаем одну RSS ленту
        					
        					//Проверяем и получаем новые записи для этой ленты
        					List<String> data = db.getNew(RSS_id,false);
        					
        					if (data==null)
        					{
        						//sendMessage("santiago26@qip.ru",RSS_id.toString()+" haz problems.");
        						//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    			            	//try{Thread.sleep(1000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
    			            	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        						sendMessage("commaster@qip.ru",RSS_id.toString()+" haz problems.");
    							//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    			            	try{Thread.sleep(1000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
    			            	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        						continue;
        					}
        					
        					//Отделяем сообщения с ббкодами и без них
        					String messages = data.get(0);
        					String messages_bboff = data.get(1);
        					
        					//--------------------------------------------------------------------------------------------------------------------        					
        					//Отправка сообщений с BB кодами
        					if(messages.length()!=0)
        					{
        						//Узнаеем кто подписан на текущую ленту и отсылаем ему сообщение с новыми новостями из ленты
        						//LOG.info("----------------[ NEW titles... ]----------------");
        						for (String jid : db.getUsersRSS(RSS_id,1,0))
        						{
        							if(!connection.isConnected()) {LOG.error("Connection dropped!");}
        							sendMessage(jid,messages);
        							//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        			            	try{Thread.sleep(5000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
        			            	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        						}
        						for (String jid : db.getUsersRSS(RSS_id,1,1))
        						{
        							if(!connection.isConnected()) {LOG.error("Connection dropped!");}
        							sendMUCBroadcast(jid,messages);
        							//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        			            	try{Thread.sleep(5000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
        			            	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        						}
        					}else{/*LOG.info("----------------[ No new titles... ]----------------");*/}
        					//--------------------------------------------------------------------------------------------------------------------
        					//Отправка сообщений без BB кодов
        					if(messages_bboff.length()!=0)
        					{
        						//Узнаеем кто подписан на текущую ленту и отсылаем ему сообщение с новыми новостями из ленты
        						//LOG.info("----------------[ NEW titles... ]----------------");
        						for (String jid : db.getUsersRSS(RSS_id,0,0))
        						{
        							if(!connection.isConnected()) {LOG.error("Connection dropped!");}
        							sendMessage(jid,messages_bboff);
        							//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        			            	try{Thread.sleep(5000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
        			            	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        						}
        						for (String jid : db.getUsersRSS(RSS_id,0,1))
        						{
        							if(!connection.isConnected()) {LOG.error("Connection dropped!");}
        							sendMUCBroadcast(jid,messages_bboff);
        							//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        			            	try{Thread.sleep(5000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
        			            	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        						}
        					}else{/*LOG.info("----------------[ No new titles... ]----------------");*/}
        					//--------------------------------------------------------------------------------------------------------------------
        					//LOG.info("RSSFeed"+id+" sent...");
        			
        				}//end for
        				LOG.info("Update finished.");
        				
        		    }catch(Exception e){LOG.error("ERROR_XMPP:",e);}
            		
            	}else if (!Stop_refresh){LOG.warn("XMPPbot is disconnected!");status = false;LOG.info("Перезапуск программы!");restartApplication();}

            	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            	try{Thread.sleep(1800000);}catch(Exception e){LOG.error("ERROR_THREAD:",e);}
            	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    			
    		}//end while
            
            
        }catch(Exception e){LOG.error("ERROR_XMPP:",e);}
        finally {
        	LOG.info("Reconnecting...");
        	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			try{Thread.sleep(60000);}catch(Exception e1){LOG.error("ERROR_THREAD:",e1);}
			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        	run();
        }
	}
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public void sendMessage(String to, String message)
	{
		if(!message.equals(""))
	    {
			LOG.info("Message to "+to);
			ChatManager chatmanager = connection.getChatManager();
	        Chat newChat = chatmanager.createChat(to, null);
	        try
	        {
	        	newChat.sendMessage(message);
	        }catch(Exception e){LOG.error("ERROR_XMPP(sendMessage):",e);}
	    }
	}
	public void leaveMUC(String MUC) {
		LOG.info("Leaving "+MUC);
		MultiUserChat mucThis = new MultiUserChat(connection,MUC);
		try {
			DiscussionHistory history = new DiscussionHistory();
            history.setMaxChars(0);
			mucThis.join("bye-bye", "", history, SmackConfiguration.getPacketReplyTimeout());
			mucThis.leave();
		}catch(XMPPException e){LOG.error("ERROR_MUCleave:",e);}
	}
	public void sendMUCMessage(String MUC, String to, String message) {
		if(!message.equals(""))
	    {
			LOG.info("Message to "+MUC+"/"+to);
			MultiUserChat mucThis = new MultiUserChat(connection,MUC);
			if (!mucThis.isJoined())
			{
				DiscussionHistory history = new DiscussionHistory();
				history.setMaxChars(0);
				try {
					mucThis.join(mucName, "", history, SmackConfiguration.getPacketReplyTimeout());
				}catch(XMPPException e){LOG.error("ERROR_MUC:",e);}
			}
            try {
            	mucThis.sendMessage(to+": "+message);
            }catch(XMPPException e){LOG.error("ERROR_MUCMes:",e);}
	    }
	}
	public void sendMUCBroadcast(String MUC, String message) {
		if(!message.equals(""))
	    {
			LOG.info("Message to "+MUC);
			MultiUserChat mucThis = new MultiUserChat(connection,MUC);
			if (!mucThis.isJoined())
			{
				DiscussionHistory history = new DiscussionHistory();
				history.setMaxChars(0);
				try {
					mucThis.join(mucName, "", history, SmackConfiguration.getPacketReplyTimeout());
				}catch(XMPPException e){LOG.error("ERROR_MUC:",e);}
			}
            try {
            	mucThis.sendMessage(message);
            }catch(XMPPException e){LOG.error("ERROR_MUCMes:",e);}
	    }
	}
	public void getRoster(String JID)
	{
		Roster roster = connection.getRoster();
        Collection<RosterEntry> entries = roster.getEntries();
        String str = "Количество: "+entries.size() + "\n";
        for(RosterEntry r:entries){str += r.getUser()+"\t";}
        sendMessage(JID, str);
	}
	public void getRevision(String JID)
	{
		String Revision = "Revision 2012.07b01-git";
		sendMessage(JID,Revision);
	}
	public void restartApplication()
	{
	  try {
		  final String javaBin = "java";
		  final String runThingy = "JabberBot.jar";
		  
		  /* Build command: java -jar application.jar */
		  final ArrayList<String> command = new ArrayList<String>();
		  command.add(javaBin);
		  command.add("-jar");
		  command.add(runThingy);
		  
		  final ProcessBuilder builder = new ProcessBuilder(command);
		  builder.start();
		  System.exit(0);
		  } catch (Exception e) {LOG.error("ERROR_GLOBAL:",e);}
	}
}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
public class MainJabberBot 
{
	public static final Logger LOG=Logger.getLogger(MainJabberBot.class);
	public static void main(String[] args) 
	{
		LOG.info("Запуск программы...");
		//Запуск потока Jabber бота	
		try
        {
			Thread BotXmpp = new Thread(new JabberBot());
			BotXmpp.start();
        }catch(Exception e){LOG.error("Ошибка запуска потока:",e);}
	}
}


