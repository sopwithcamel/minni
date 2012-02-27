#ifndef LIB_COMPRESS_SLAVES_H
#define LIB_COMPRESS_SLAVES_H
#include <iostream>
#include <stdint.h>
#include <string>
#include <vector>

#include "CompressTree.h"
#include "CTNode.h"
#include "EmptyQueue.h"

namespace compresstree {
    class CompressTree;
    class Node;

    class Slave {
        friend class CompressTree;
        friend class Node;
      public:
        Slave(CompressTree* tree);
        virtual ~Slave() {}
        virtual void* work() = 0;
        virtual void addNode(Node* node) = 0;
        virtual bool empty() const;
        virtual bool wakeup();    
        virtual void setInputComplete(bool value) { inputComplete_ = value; }
        virtual void sendCompletionNotice();
        virtual void waitUntilCompletionNoticeReceived();

        virtual void printElements() const;
      protected:
        CompressTree* tree_;
        bool inputComplete_;
        bool queueEmpty_;

        pthread_t thread_;
        pthread_cond_t queueHasWork_;
        pthread_mutex_t queueMutex_;

        pthread_mutex_t completionMutex_;
        pthread_cond_t complete_;
        bool askForCompletionNotice_;

        std::deque<Node*> nodes_;
    };

    class Emptier : public Slave {
        friend class CompressTree;
        friend class Node;
        struct PrioComp {
            bool operator()(uint32_t lhs, uint32_t rhs)
            {
                return (lhs > rhs);
            }
        };
      public:
        Emptier(CompressTree* tree);
        ~Emptier();
        void* work();
        void addNode(Node* node);
      private:
        EmptyQueue queue_;
    };

    class Compressor : public Slave {
        friend class CompressTree;
        friend class Node;
      public:
        Compressor(CompressTree* tree);
        ~Compressor();
        void* work();
        void addNode(Node* node);
    };

    class Sorter : public Slave {
        friend class CompressTree;
        friend class Node;
      public:
        Sorter(CompressTree* tree);
        ~Sorter();
        void* work();
        void addNode(Node* node);
    };

    class Pager : public Slave {
        friend class CompressTree;
        friend class Node;
      public:
        Pager(CompressTree* tree);
        ~Pager();
        void pageOut(Node* node);
        void pageIn(Node* node);
        void* work();
        void addNode(Node* node);
    };
}

#endif
