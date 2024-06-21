// Author: Baruch Sterin <baruchs@gmail.com>

#include <Python.h>

#include <string>
#include <vector>
#include <thread>

using namespace std;

class SubInterpreter
{
public:
    SubInterpreter()
    {
        PyInterpreterConfig config = {
            .use_main_obmalloc = 0,
            .allow_fork = 0,
            .allow_exec = 0,
            .allow_threads = 0,
            .allow_daemon_threads = 0,
            .check_multi_interp_extensions = 1,
            .gil = PyInterpreterConfig_OWN_GIL,
        };
        Py_NewInterpreterFromConfig(&_ts, &config);
        // _ts->interp->strict_extension_compat = false;
    }
    ~SubInterpreter()
    {
        if (_ts)
        {
            Py_EndInterpreter(_ts);
        }
    }
    void swap_to_self()
    {
        PyThreadState_Swap(_ts);
    }

private:
    PyThreadState *_ts;
};

void f(const char *tname)
{
    SubInterpreter sub;
    // sub.swap_to_self();
    std::string code = R"PY(
#import numpy as np        
#print(f"sub {np.__version__}")
print(f"TNAME gan ni niang")

a = 0
for i in range(10000000):
    a = a+1
print("finished")
    )PY";

    code.replace(code.find("TNAME"), 5, tname);
    PyRun_SimpleString(code.c_str());
}

int main()
{
    Py_Initialize();
    PyGILState_STATE gstate = PyGILState_Ensure();
    std::string code = R"PY(
import numpy as np
print(f"main {np.__version__}")
arr = 12138
print(f"TNAME  Values in the array: {arr}")
    )PY";
    code.replace(code.find("TNAME"), 5, "main_interpreter");
    PyRun_SimpleString(code.c_str());
    // SubInterpreter s1, s2, s3, s4;
    std::thread t1{f, "t1___"};
    std::thread t2{f, "t2___"};
    std::thread t3{f, "t3___"};
    std::thread t4{f, "t4___"};
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    Py_Finalize();
    return 0;
}
