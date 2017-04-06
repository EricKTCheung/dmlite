
#include "MySqlIO.h"

#include "sys/param.h"
#include "NsMySql.h"

// ------------------------------------------------
// MysqlIODriverPassthrough
//




MysqlIOPassthroughDriver::MysqlIOPassthroughDriver(IODriver* decorates, int maxdirspacereportdepth) throw (DmException) {
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Ctor");

  this->dirspacereportdepth = maxdirspacereportdepth;
  this->decorated_   = decorates;
  this->decoratedId_ = strdup( decorates->getImplId().c_str() );
}

MysqlIOPassthroughDriver::~MysqlIOPassthroughDriver() {

  delete this->decorated_;
  free(this->decoratedId_);

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "");
}

IOHandler* MysqlIOPassthroughDriver::createIOHandler(const std::string& pfn,
                                       int flags,
                                       const Extensible& extras,
                                       mode_t mode) throw (DmException) {


      Log(Logger::Lvl4, mysqllogmask, mysqllogname, "pfn: " << pfn << " flags: " << flags);
      return decorated_->createIOHandler(pfn, flags, extras, mode);

}

void MysqlIOPassthroughDriver::setStackInstance(StackInstance* si) throw (DmException) {
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  BaseInterface::setStackInstance(this->decorated_, si);
  this->stack_ = si;
}


void MysqlIOPassthroughDriver::setSecurityContext(const SecurityContext* ctx) throw (DmException) {
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  BaseInterface::setSecurityContext(this->decorated_, ctx);
}


void MysqlIOPassthroughDriver::doneWriting(const Location& loc) throw (DmException) {
  Log(Logger::Lvl1, mysqllogmask, mysqllogname, " loc:" << loc.toString());
  ExtendedStat st;

  decorated_->doneWriting(loc);

  // Semantically speaking, here the file size should be available

  // Stat the file to have the size of the file that was just written
  try {
    Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Looking up size of " << loc[0].url.query.getString("sfn"));
    st = this->stack_->getCatalog()->extendedStat(loc[0].url.query.getString("sfn"));
    Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Ok. Size of " << loc[0].url.query.getString("sfn") << " is " << st.stat.st_size);
  }
  catch (DmException& e) {
    Err( "MysqlIOPassthroughDriver::doneWriting" , " Cannot retrieve filesize for loc:" << loc.toString());
    return;
  }

  INodeMySql *inodeintf = dynamic_cast<INodeMySql *>(this->stack_->getINode());
  if (!inodeintf) {
    Err( "MysqlIOPassthroughDriver::doneWriting" , " Cannot retrieve inode interface. Fatal.");
    return;
  }

  if (st.parent <= 0) {

    try {
      Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Looking up parent of inode " << st.stat.st_ino << " " << loc[0].url.query.getString("sfn"));
      st = inodeintf->extendedStat(st.stat.st_ino);
      Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Ok. Parent of  inode " << st.stat.st_ino << " is " << st.parent);
    }
    catch (DmException& e) {
      Err( "MysqlIOPassthroughDriver::doneWriting" , " Cannot retrieve parent for loc:" << loc.toString());
      return;
    }

  }


  // Add this filesize to the size of its parent dirs, only the first N levels
  {


    // Start transaction
    InodeMySqlTrans trans(inodeintf);

    off_t sz = st.stat.st_size;


    ino_t hierarchy[128];
    size_t hierarchysz[128];
    int idx = 0;
    while (st.parent) {

      Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Going to stat " << st.parent << " parent of " << st.stat.st_ino << " with idx " << idx);

      try {
        st = inodeintf->extendedStat(st.parent);
      }
      catch (DmException& e) {
        Err( "MysqlIOPassthroughDriver::doneWriting" , " Cannot stat inode " << st.parent << " parent of " << st.stat.st_ino);
        return;
      }

      hierarchy[idx] = st.stat.st_ino;
      hierarchysz[idx] = st.stat.st_size;

      Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Size of inode " << st.stat.st_ino <<
      " is " << st.stat.st_size << " with idx " << idx);

      idx++;

      if (idx >= (int)sizeof(hierarchy)) {
        Err( "MysqlIOPassthroughDriver::doneWriting" , " Too many parent directories for replica " << loc.toString());
        return;
      }
    }

    // Update the filesize in the first levels
    // Avoid the contention on /dpm/voname/home
    if (idx > 0) {
      Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Going to set sizes. Max depth found: " << idx);
      for (int i = MAX(0, idx-3); i >= MAX(0, idx-1-dirspacereportdepth); i--) {
        inodeintf->setSize(hierarchy[i], sz + hierarchysz[i]);
      }
    }
    else {
      Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Cannot set any size. Max depth found: " << idx);
    }


    // Commit the local trans object
    // This also releases the connection back to the pool
    trans.Commit();
  }
}
