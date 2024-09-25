#include <clang/AST/ASTConsumer.h>
#include <clang/AST/RecursiveASTVisitor.h>
#include <clang/Frontend/ASTConsumers.h>
#include <clang/Frontend/FrontendActions.h>
#include <clang/Tooling/CommonOptionsParser.h>
#include <clang/Tooling/Tooling.h>
#include <clang/Frontend/CompilerInstance.h>
#include <iostream>

using namespace clang;
using namespace clang::tooling;

class FunctionVisitor : public RecursiveASTVisitor<FunctionVisitor> {
public:
    explicit FunctionVisitor(ASTContext *context) : context(context) {}

    bool VisitFunctionDecl(FunctionDecl *func) {
        // 関数名を取得
        std::string funcName = func->getNameInfo().getName().getAsString();

        // noexcept が指定されているかどうかを確認
        bool isNoexcept = (func->getExceptionSpecType() == EST_BasicNoexcept);

        // 結果を出力
        std::cout << "Function: " << funcName 
                  << " - noexcept: " << (isNoexcept ? "true" : "false") 
                  << std::endl;

        return true;
    }

private:
    ASTContext *context;
};

class FunctionASTConsumer : public ASTConsumer {
public:
    explicit FunctionASTConsumer(ASTContext *context) : visitor(context) {}

    virtual void HandleTranslationUnit(ASTContext &context) override {
        visitor.TraverseDecl(context.getTranslationUnitDecl());
    }

private:
    FunctionVisitor visitor;
};

class FunctionFrontendAction : public ASTFrontendAction {
public:
    virtual std::unique_ptr<ASTConsumer> CreateASTConsumer(CompilerInstance &CI, StringRef file) override {
        return std::make_unique<FunctionASTConsumer>(&CI.getASTContext());
    }
};

static llvm::cl::OptionCategory MyToolCategory("my-tool options");

int main(int argc, const char **argv) {
    auto optionsParser = CommonOptionsParser::create(argc, argv, MyToolCategory);
    if (!optionsParser) {
        llvm::errs() << "Error creating CommonOptionsParser\n";
        return 1;
    }

    ClangTool tool(optionsParser->getCompilations(), optionsParser->getSourcePathList());
    return tool.run(newFrontendActionFactory<FunctionFrontendAction>().get());
}

