# Changelog before first commit

###2012.07b01 (14.07.2012):
+ MUC JID logging.

###2012.06b03 (01.06.2012):
+ Outgoing log transfer.

###2012.06b01 (01.06.2012):
+ Removed “Автор: UNKNOWN”.

###2012.05b30 (29.05.2012):
+ Fixed home-link.

###2012.05b29 (27.05.2012):
+ Protection for inconsistent feed-type.

###2012.05b28 (27.05.2012):
+ <Updated> field support.

###2012.05b26 (26.05.2012):
+ Reconnection undead patch.
+ 1 minute freeze for next attempt.

###2012.05b20 (12.05.2012):
+ Unified message processing.

###2012.05b19 (12.05.2012):
+ More admin commands (slist, slas, sl, listerrors, listerrorsbb)...
+ Direct SQL (SQLQuery, SQLUpdate).
+ Switching Invitation processing...

###2012.04b93 (30.04.2012):
+ Disabled feed timeout. (fixed MUC messaging)
+ RSS error manipulation and listing commands for admin.
+ join MUC for users to join uninvitable MUCs.

###2012.04b81 (21.04.2012):
+ Feed fetch timeout at 10 seconds.

###2012.04b80 (20.04.2012):
+ Converted sudo to pure-admin commands.
+ Remote list command.
+ Remote user removal.
+ Remote user registration.
+ Admin and undoc help.
+ Registered users display.
+ Proper type in list subscription id.
+ Fixed entering a muc, that’s vanished.
+ Exact message type filter.
+ Reworked refresh to prevent DB deadlock on web deadlock.

###2012.04b63 (15.04.2012):
+ MUC leave.
+ Fixed “delete empty” deadlock.

###2012.04b61 (15.04.2012):
+ User removal.

###2012.04b58 (15.04.2012):
+ Welcome to Groupchats!@!@!!

###2012.04b55 (13.04.2012):
+ Fixed doubleposting for non-default type feeds.
+ Fixed blacken last save for non-default feeds.

###2012.04b53 (12.04.2012):
+ Ignoring badly constructed typing notifications (aka “Empties”)

###2012.04b51 (12.04.2012):
+ sudo-command db.rehead.
+ Empty message debugging.
+ l <RSS_id> to look at the last post.

###2012.04b42 (12.04.2012):
+ More tolerated error-forgiving.
+ Fixed DB-contents in preparation for last <RSS_id>.

###2012.04b40 (11.04.2012):
+ Second chance for feeds to skip random one-time failures.

###2012.04b34 (08.04.2012):
+ Fix: Error notification failed halfway.

###2012.04b33 (08.04.2012):
+ Fix error logging.
+ Fix BLOB error storage.

###2012.04b31 (08.04.2012):
+ +1 sudo-command(db.getlink RSS_id).
+ Trying to make BLOB-storage readable...

###2012.04b27 (07.04.2012):
+ Made all that shit up there actually work!

###2012.04b11 (07.04.2012):
+ SMACK 3.2.2

###2012.04b10 (07.04.2012):
+ sudo-help.

###2012.04b09 (07.04.2012):
+ Migrated to new DB structure.
+ Updated IDE to Juno M6.
+ Fixed Author detection.
+ New, categorized, RSS-tree parser.
+ User-accounts.
+ Tag detection.(i.e. habrahabr)
+ Synchronized DB access.
+ sudo-commands(db.ping,db.geterror RSS_id,db.pardon RSS_id).
+ Automated feed-error protection.

###2012.02b23 (14.02.2012):
+ Basic groupchat support. (multi-instanced, cross-domain)

###2012.02b10 (14.02.2012):
+ Flood-proof for Stupid-proof.
+ Message cooldown is lowered to 5 sec.

###2012.02b08 (14.02.2012):
+ Beauty-fix for Stupid-proof replies.

###2012.02b06 (14.02.2012):
+ Stupid-proof for wrong messages.

###2012.02b03 (10.02.2012):
+ Deleting subscriptions seems to work again... Yay!

###2012.02b02 (09.02.2012):
+ reordered database access.
+ reduced memory consumption.
+ Found out, that there was no memory leak... It’s just the way this language works...

###2012.02b01 (05.02.2012):
+ Updated IDE to Juno M5.
+ undead mod v2.

###2012.01b15 (18.01.2012):
+ ideas are sent to Santiago26.
+ ideas say a Thank you! to the user.

###2012.01b14 (18.01.2012):
+ Help for newcommers.
+ Properly escaped single quotes! (Hurray! Hellz Yeah!)
+ Replaced </div> with a newline.

###2012.01b13 (16.01.2012):
+ undead mod.