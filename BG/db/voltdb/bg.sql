CREATE TABLE Friendship (
userid1 INTEGER NOT NULL,
userid2 INTEGER NOT NULL,
status INTEGER NOT NULL,
PRIMARY KEY (userid1,userid2));

CREATE TABLE Users (
userid INTEGER NOT NULL,
username VARCHAR(255),
pw VARCHAR(255),
fname VARCHAR(255),
lname VARCHAR(255),
gender VARCHAR(255),
dob VARCHAR(255),
jdate VARCHAR(255),
ldate VARCHAR(255),
address VARCHAR(255),
email VARCHAR(255),
tel VARCHAR(255),
confirmedFriends INTEGER,
pendingFriends INTEGER,
resourceCount INTEGER,
PRIMARY KEY (userid));

CREATE INDEX i2 ON Users (confirmedFriends,pendingFriends,resourceCount);

CREATE TABLE Resource (
rid INTEGER NOT NULL,
creatorid INTEGER,
wallUserId INTEGER NOT NULL,
type VARCHAR(255),
body VARCHAR(255),
doc VARCHAR(255),
PRIMARY KEY (rid));

CREATE INDEX i3 ON Resource (wallUserId);

PARTITION TABLE Resource ON COLUMN wallUserId;

CREATE TABLE Modify (
creatorid INTEGER,
rid INTEGER NOT NULL,
modifierid INTEGER,
timestamp VARCHAR(255),
type VARCHAR(255),
content VARCHAR(255)); 

CREATE INDEX i4 ON Modify (rid);

PARTITION TABLE Modify ON COLUMN rid;

CREATE PROCEDURE FROM CLASS voltdbDataStore.AcceptFriend;

CREATE PROCEDURE FROM CLASS voltdbDataStore.DeleteFriendship;

CREATE PROCEDURE FROM CLASS voltdbDataStore.FriendsPerUser;

CREATE PROCEDURE FROM CLASS voltdbDataStore.GetCreatedResources;

CREATE PROCEDURE FROM CLASS voltdbDataStore.InsertFriends;

CREATE PROCEDURE FROM CLASS voltdbDataStore.InsertPendingFriends;

CREATE PROCEDURE FROM CLASS voltdbDataStore.InsertResources;

CREATE PROCEDURE FROM CLASS voltdbDataStore.InsertUser;

CREATE PROCEDURE FROM CLASS voltdbDataStore.ListFriends;

CREATE PROCEDURE FROM CLASS voltdbDataStore.PendingFriendsPerUser;

CREATE PROCEDURE FROM CLASS voltdbDataStore.PostCommentOnResource;

CREATE PROCEDURE FROM CLASS voltdbDataStore.ProfileDetails;

CREATE PROCEDURE FROM CLASS voltdbDataStore.ResourceCount;

CREATE PROCEDURE FROM CLASS voltdbDataStore.Select;

CREATE PROCEDURE FROM CLASS voltdbDataStore.SelectUserCount;

CREATE PROCEDURE FROM CLASS voltdbDataStore.ThawFriendship;

CREATE PROCEDURE FROM CLASS voltdbDataStore.UpdateUserForConfirmedFriends;

CREATE PROCEDURE FROM CLASS voltdbDataStore.UpdateUserForResources;

CREATE PROCEDURE FROM CLASS voltdbDataStore.UserOffset;

CREATE PROCEDURE FROM CLASS voltdbDataStore.ViewCommentOnResource;

CREATE PROCEDURE FROM CLASS voltdbDataStore.ViewFriendReq;

CREATE PROCEDURE FROM CLASS voltdbDataStore.ViewTopKResources;

