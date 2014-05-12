/// @file    plugins/proc/Provider.h
/// @brief   Base provider
/// @author  Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef PROVIDER_H
#define PROVIDER_H

namespace dmlite {

/// Represents a proc provider entry
/// Default implementation is a collection
struct ProcProvider {
  std::string name;
  std::list<ProcProvider*> children;
  StackInstance* stack;

  ProcProvider(const std::string& name): name(name), stack(NULL) {}

  virtual ~ProcProvider() {
    std::list<ProcProvider*>::iterator i;
    for (i = this->children.begin(); i != this->children.end(); ++i) {
      delete *i;
    }
  }

  void setStackInstance(StackInstance* si)
  {
    this->stack = si;
    std::list<ProcProvider*>::iterator i;
    for (i = this->children.begin(); i != this->children.end(); ++i) {
      (*i)->setStackInstance(si);
    }
  }

  virtual ExtendedStat stat() const {
    ExtendedStat xs;
    xs.name = this->name;
    xs.status = dmlite::ExtendedStat::kOnline;
    ::memset(&xs.stat, 0, sizeof(xs.stat));
    xs.stat.st_mode = S_IFDIR | 0555;
    xs.stat.st_nlink = children.size();
    return xs;
  }

  virtual std::string getContent() const {
    return std::string("ProcProvider");
  }

  static ProcProvider* follow(ProcProvider* root, const std::string& cwd, const std::string& path)
  {
    if (path.size() == 0)
      throw DmException(EINVAL, "Invalid empty path");

    std::string fullPath;
    if (path[0] == '/') {
      fullPath = path;
    }
    else if (!cwd.empty()) {
      fullPath = cwd + "/" + path;
    }
    else {
      fullPath = std::string("/") + path;
    }

    std::vector<std::string> components = Url::splitPath(fullPath);
    ProcProvider* item = root;

    std::vector<std::string>::const_iterator i;
    for (i = components.begin(); i != components.end(); ++i) {
      std::list<ProcProvider*>* children;
      std::list<ProcProvider*>::iterator j;

      if (i->compare("/") != 0 && i->compare(".") != 0) {
        children = &item->children;
        item = NULL;

        for (j = children->begin(); j != children->end(); ++j) {
          if (i->compare((*j)->name) == 0) {
            item = *j;
            break;
          }
        }
      }
      if (!item)
        throw DmException(ENOENT, "Could not find " + path);
    }

    return item;
  }

};

};

#endif // PROVIDER_H
