#include <QDir>
#include <QFile>
#include <QPointer>
#include <QProcess>
#include <QSqlQuery>
#include <QTimeZone>
#include <functional>
#include <QSqlDatabase>
#include <QJsonArray>
#include <QJsonObject>
#include <QJsonDocument>
#include <QJsonParseError>
#include <QNetworkReply>
#include <QCoreApplication>
#include <QNetworkAccessManager>
#include <QtConcurrent/QtConcurrentMap>

QString simplifyTags(const QJsonArray &tags)
{   // tag处理
    if (tags.isEmpty()) return {};
    QJsonObject tagObject;
    for (const auto &tag : tags) {
        QJsonObject obj = tag.toObject();
        QString name = obj["name"].toString().trimmed();
        if (!name.isEmpty()) tagObject[name] = obj["count"].toInt();
    }
    return QJsonDocument(tagObject).toJson(QJsonDocument::Compact);
}

qint64 dateStringToTimestamp(const QString& dateStr)
{   // yyyy-MM-dd -> Unix时间戳
    if (dateStr.isEmpty()) return 0;
    const QDate date = QDate::fromString(dateStr, "yyyy-MM-dd");
    const QDateTime dt(date.startOfDay(QTimeZone::utc()));
    return dt.toSecsSinceEpoch();
}

QByteArray compressString(const QString &str)
{   // 压缩字符串
    const QByteArray original = str.toUtf8();
    return qCompress(original, 9);
}

QString extractChineseNameFromInfobox(const QString &infobox)
{
    if (infobox.isEmpty()) return {};
    const QStringList lines = infobox.split('\n', Qt::SkipEmptyParts);
    for (const QString &line : lines) {
        QString trimmed = line.trimmed();
        if (trimmed.startsWith("|简体中文名=")) {
            QString value = trimmed.mid(QString("|简体中文名=").length()).trimmed();
            if (value.endsWith('}')) value.chop(1);
            return value;
        }
    }
    return {};
}

bool insertEpisodeAirdateFromFile(const QString &filePath, QSqlDatabase db)
{   // episode公共数据插入
    QFile file(filePath);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) return false;
    QSqlQuery query(db);
    query.prepare("INSERT OR REPLACE INTO episode_public_date (subject_id, id, airdate, sort) VALUES (?,?,?,?)");
    db.transaction();
    int count = 0;
    constexpr int batchSize = 10000;
    QTextStream stream(&file);
    while (!stream.atEnd()) {
        QString line = stream.readLine();
        if (line.isEmpty()) continue;
        QJsonDocument doc = QJsonDocument::fromJson(line.toUtf8());
        if (!doc.isObject()) continue;
        QJsonObject obj = doc.object();
        QString airdateStr = obj["airdate"].toString();
        if (airdateStr.isEmpty()) continue;
        const int subjectId = obj["subject_id"].toInt();
        const int id = obj["id"].toInt();
        const int sort = static_cast<int>(obj["sort"].toDouble() * 10.0);
        query.addBindValue(subjectId);
        query.addBindValue(id);
        query.addBindValue(dateStringToTimestamp(airdateStr));
        query.addBindValue(sort);
        query.exec();
        ++count;
        if (count % batchSize == 0) {
            db.commit();
            db.transaction();
        }
    }
    db.commit();
    return true;
}

bool insertSubjectPublic(const QString& filePath, QSqlDatabase db, const QList<int>& allowedTypes)
{   // subject公共数据插入
    QFile file(filePath);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) return false;
    QSqlQuery query(db);
    query.prepare("INSERT OR REPLACE INTO subject_public_date VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
    db.transaction();
    int count = 0;
    constexpr int batchSize = 10000;
    QTextStream stream(&file);
    while (!stream.atEnd()) {
        QString line = stream.readLine().trimmed();
        if (line.isEmpty()) continue;
        QJsonParseError parseError;
        QJsonDocument doc = QJsonDocument::fromJson(line.toUtf8(), &parseError);
        if (parseError.error != QJsonParseError::NoError || !doc.isObject()) continue;
        QJsonObject obj = doc.object();
        if (!allowedTypes.contains(obj["type"].toInt())) continue;
        query.addBindValue(obj["id"].toInt());
        query.addBindValue(obj["name"].toString());
        query.addBindValue(obj["name_cn"].toString());
        query.addBindValue(compressString(obj["summary"].toString()));
        query.addBindValue(simplifyTags(obj["tags"].toArray()));
        query.addBindValue(QJsonDocument(obj["meta_tags"].toArray()).toJson(QJsonDocument::Compact));
        query.addBindValue(obj["score"].toDouble() * 10.0);
        query.addBindValue(obj["rank"].toInt());
        query.addBindValue(dateStringToTimestamp(obj["date"].toString()));
        QJsonObject scoreDetails = obj["score_details"].toObject();
        int totalVotes = 0;
        for (auto it = scoreDetails.begin(); it != scoreDetails.end(); ++it) totalVotes += it.value().toInt();
        query.addBindValue(totalVotes);
        QJsonObject favorite = obj["favorite"].toObject();
        query.addBindValue(favorite["doing"].toInt());
        query.addBindValue(favorite["done"].toInt());
        query.addBindValue(favorite["dropped"].toInt());
        query.addBindValue(favorite["on_hold"].toInt());
        query.addBindValue(favorite["wish"].toInt());
        query.exec();
        ++count;
        if (count % batchSize == 0) {
            db.commit();
            db.transaction();
        }
    }
    db.commit();
    return true;
}

bool insertCharacterPublic(const QString &filePath, QSqlDatabase db)
{
    QFile file(filePath);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) return false;
    QSqlQuery query(db);
    query.prepare("INSERT OR REPLACE INTO character_public_date (id, name, name_cn) VALUES (?,?,?)");
    db.transaction();
    int count = 0;
    constexpr int batchSize = 10000;
    QTextStream stream(&file);
    while (!stream.atEnd()) {
        QString line = stream.readLine().trimmed();
        if (line.isEmpty()) continue;
        QJsonParseError parseError;
        QJsonDocument doc = QJsonDocument::fromJson(line.toUtf8(), &parseError);
        if (parseError.error != QJsonParseError::NoError || !doc.isObject()) continue;
        QJsonObject obj = doc.object();
        const int id = obj["id"].toInt();
        QString name = obj["name"].toString().trimmed();
        QString infobox = obj["infobox"].toString();
        QString name_cn = extractChineseNameFromInfobox(infobox);
        query.addBindValue(id);
        query.addBindValue(name);
        query.addBindValue(name_cn);
        query.exec();
        ++count;
        if (count % batchSize == 0) {
            db.commit();
            db.transaction();
        }
    }
    db.commit();
    return true;
}

bool insertSubjectCharacter(const QString &filePath, QSqlDatabase db)
{
    QFile file(filePath);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) return false;
    QSqlQuery query(db);
    query.prepare("INSERT OR REPLACE INTO subject_character (subject_id, character_id, type) VALUES (?,?,?)");
    db.transaction();
    int count = 0;
    constexpr int batchSize = 10000;
    QTextStream stream(&file);
    while (!stream.atEnd()) {
        QString line = stream.readLine().trimmed();
        if (line.isEmpty()) continue;
        QJsonParseError parseError;
        QJsonDocument doc = QJsonDocument::fromJson(line.toUtf8(), &parseError);
        if (parseError.error != QJsonParseError::NoError || !doc.isObject()) continue;
        QJsonObject obj = doc.object();
        const int subjectId = obj["subject_id"].toInt();
        const int characterId = obj["character_id"].toInt();
        const int type = obj["type"].toInt();
        query.addBindValue(subjectId);
        query.addBindValue(characterId);
        query.addBindValue(type);
        query.exec();
        ++count;
        if (count % batchSize == 0) {
            db.commit();
            db.transaction();
        }
    }
    db.commit();
    return true;
}

QString fetchBrowserDownloadUrl()
{
    QNetworkAccessManager manager;
    const QNetworkRequest request(QUrl("https://raw.githubusercontent.com/bangumi/Archive/refs/heads/master/aux/latest.json"));
    QNetworkReply *reply = manager.get(request);
    QEventLoop loop;
    QObject::connect(reply, &QNetworkReply::finished, &loop, &QEventLoop::quit);
    loop.exec();
    if (reply->error() != QNetworkReply::NoError) {
        qDebug() << reply->errorString();
        reply->deleteLater();
        return {};
    }
    const QByteArray data = reply->readAll();
    reply->deleteLater();
    QString url = QJsonDocument::fromJson(data).object()["browser_download_url"].toString();
    return url;
}

bool downloadFile(const QString &url, const QString &localPath)
{   // 下载文件
    QNetworkAccessManager manager;
    const QNetworkRequest request(url);
    QNetworkReply *reply = manager.get(request);
    QEventLoop loop;
    QFile file(localPath);
    if (!file.open(QIODevice::WriteOnly)) {
        reply->deleteLater();
        return false;
    }
    QPointer filePtr = &file;
    QObject::connect(reply, &QNetworkReply::readyRead, [filePtr, reply] {if (filePtr) filePtr->write(reply->readAll());});
    QObject::connect(reply, &QNetworkReply::finished, &loop, &QEventLoop::quit);
    loop.exec();
    if (reply->bytesAvailable() > 0) file.write(reply->readAll());
    file.close();
    const bool success = reply->error() == QNetworkReply::NoError;
    reply->deleteLater();
    return success;
}

bool extractZip(const QString &zipPath, const QString &destDir, QString &episodePath, QString &subjectPath, QString &characterPath, QString &subjectCharacterPath)
{
    const QDir dir;
    if (!dir.mkpath(destDir)) return false;
    QProcess unzip;
    QStringList args;
    args << "-o" << zipPath << "-d" << destDir;
    unzip.start("unzip", args);
    if (!unzip.waitForFinished()) return false;
    episodePath = destDir + "/episode.jsonlines";
    subjectPath = destDir + "/subject.jsonlines";
    characterPath = destDir + "/character.jsonlines";
    subjectCharacterPath = destDir + "/subject-characters.jsonlines";
    return true;
}

void closeAndRemoveDatabase(const QString &connectionName)
{
    {
        QSqlDatabase db = QSqlDatabase::database(connectionName, false);
        if (db.isValid()) db.close();
    }
    QSqlDatabase::removeDatabase(connectionName);
}

int main(int argc, char *argv[])
{
    QCoreApplication app(argc, argv);
    const QString downloadUrl = fetchBrowserDownloadUrl();
    if (downloadUrl.isEmpty()) return 1;
    QString fileName = QUrl(downloadUrl).fileName();
    if (fileName.isEmpty()) fileName = "data.zip";
    const QString zipPath = QCoreApplication::applicationDirPath() + "/" + fileName;
    if (!downloadFile(downloadUrl, zipPath)) return 1;
    const QString extractDir = QCoreApplication::applicationDirPath() + "/extracted";
    QString episodeFile, subjectFile, characterFile, subjectCharacterFile;
    if (!extractZip(zipPath, extractDir, episodeFile, subjectFile, characterFile, subjectCharacterFile)) return 1;
    QList<QList<int>> typeCombinations = {{1}, {2}, {4}, {1,2}, {1,4}, {2,4}, {1,2,4}};
    QStringList dbNames = {
        "public_date_1.db", "public_date_2.db", "public_date_4.db",
        "public_date_12.db", "public_date_14.db", "public_date_24.db",
        "public_date_124.db"
    };
    QList< std::function<void()> > tasks;
    for (int i = 0; i < typeCombinations.size(); ++i) {
        const auto& types = typeCombinations[i];
        const QString& dbName = dbNames[i];
        const QString dbPath = QCoreApplication::applicationDirPath() + "/" + dbName;
        tasks.append([=] {
            const QString connName = QString("public_date_connection_%1").arg(reinterpret_cast<quintptr>(QThread::currentThreadId()));
            {
                QSqlDatabase db = QSqlDatabase::addDatabase("QSQLITE", connName);
                db.setDatabaseName(dbPath);
                if (!db.open()) return;
                const QStringList publicTables = {
                    "CREATE TABLE IF NOT EXISTS episode_public_date ("
                    "subject_id INTEGER, id INTEGER, airdate INTEGER, sort INTEGER, PRIMARY KEY (subject_id, id))",
                    "CREATE TABLE IF NOT EXISTS subject_public_date ("
                    "id INTEGER PRIMARY KEY, name TEXT, name_cn TEXT, summary BLOB, tags TEXT, meta_tags TEXT, score INTEGER, rank INTEGER, date INTEGER, rating_total INTEGER, doing INTEGER, done INTEGER, dropped INTEGER, on_hold INTEGER, wish INTEGER)",
                    "CREATE TABLE IF NOT EXISTS character_public_date ("
                    "id INTEGER PRIMARY KEY, name TEXT, name_cn TEXT)",
                    "CREATE TABLE IF NOT EXISTS subject_character ("
                    "subject_id INTEGER, character_id INTEGER, type INTEGER, PRIMARY KEY (subject_id, character_id)"
                };
                QSqlQuery publicQuery(db);
                for (const auto &sql : publicTables) publicQuery.exec(sql);
                if (!insertEpisodeAirdateFromFile(episodeFile, db)) {
                    qDebug() << QThread::currentThreadId() << "插入 episode 失败";
                    db.close();
                    QSqlDatabase::removeDatabase(connName);
                    return;
                }
                if (!insertCharacterPublic(characterFile, db)) {
                    qDebug() << QThread::currentThreadId() << "插入 character 失败";
                    db.close();
                    QSqlDatabase::removeDatabase(connName);
                    return;
                }
                if (!insertSubjectPublic(subjectFile, db, types)) {
                    qDebug() << QThread::currentThreadId() << "插入 subject 失败";
                    db.close();
                    QSqlDatabase::removeDatabase(connName);
                    return;
                }
                if (!insertSubjectCharacter(subjectFile, db)) {
                    qDebug() << QThread::currentThreadId() << "插入 Subject_Character 失败";
                    db.close();
                    QSqlDatabase::removeDatabase(connName);
                    return;
                }
                db.close();
            }
            QSqlDatabase::removeDatabase(connName);
            qDebug() << dbName << "完成";
        });
    }
    QThreadPool::globalInstance()->setMaxThreadCount(7);
    QtConcurrent::blockingMap(tasks, [](const std::function<void()>& task) {task();});
    return 0;
}