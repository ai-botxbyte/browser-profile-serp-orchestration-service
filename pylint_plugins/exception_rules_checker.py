# pylint_plugins/exception_rules_checker.py
import astroid
from pylint.checkers import BaseChecker
from pylint.lint import PyLinter


class ExceptionAndKeywordChecker(BaseChecker):
    """Custom Pylint checker enforcing  exception conventions and keyword-only args."""

    name = "exception-keyword-checker"
    priority = -1
    msgs = {
        # Exception inheritance and naming rules
        "E9950": (
            "Exception class '%s' must end with 'Exception'",
            "exception-name-must-end-with-exception",
            "All exception classes must end with 'Exception'.",
        ),
        "E9951": (
            "BaseAppException must be in 'app/exception/baseapp_exception.py'",
            "baseappexception-location-invalid",
            "BaseAppException should be located in app/exception/baseapp_exception.py.",
        ),
        "E9952": (
            "Exception file '%s' must only import from baseapp_exception.py",
            "exception-import-violation",
            "Exception files should only import BaseAppException from baseapp_exception.py.",
        ),

        # Status code rules
        "E9954": (
            "Non-HTTPException subclass must use 'message=message' in super().__init__()",
            "non-http-exception-use-message",
            "Classes not inheriting from HTTPException should always pass message=message.",
        ),
        "E9955": (
            "HTTPException raise statements must use 'detail=' keyword argument",
            "http-exception-raise-must-use-detail",
            "When raising HTTPException, always use detail= keyword argument: raise HTTPException(status_code=..., detail=...)",
        ),
        "E9956": (
            "HTTPException __init__ must not use 'detail' parameter directly",
            "http-exception-init-must-accept-message",
            "HTTPException subclasses should not use 'detail' parameter directly. Use 'message' parameter instead or construct message internally.",
        ),
    }

    # ---------------- Utility helpers ----------------
    def _is_exception_file(self, node: astroid.NodeNG) -> bool:
        """Return True if file looks like an exception module."""
        filename = node.root().file or ""
        return filename.endswith("_exception.py")

    def _is_baseapp_exception_file(self, node: astroid.NodeNG) -> bool:
        """Return True if file is the baseapp_exception.py file."""
        filename = node.root().file or ""
        return filename.endswith("baseapp_exception.py")

    def _inherits_from_httpexception(self, node: astroid.ClassDef) -> bool:
        """Check if a class inherits from HTTPException (directly or through BaseAppException)."""
        try:
            # Check direct bases
            for base in node.bases:
                base_name = base.as_string()
                if "HTTPException" in base_name:
                    return True
                
                # Check if inheriting from BaseAppException, which inherits from HTTPException
                if "BaseAppException" in base_name:
                    return True
            
            # Try to infer ancestors
            for ancestor in node.ancestors():
                if ancestor.name in ("HTTPException", "BaseAppException"):
                    return True
        except (astroid.InferenceError, AttributeError, TypeError):
            pass
        
        return False

    # ---------------- Checks ----------------
    def _check_init_signature(self, node: astroid.ClassDef):
        """Check that HTTPException subclasses don't use 'detail' parameter directly."""
        # Skip BaseAppException itself and non-HTTPException classes
        if node.name == "BaseAppException":
            return
            
        if not self._inherits_from_httpexception(node):
            return

        # Look for __init__ method directly in this class
        for child in node.body:
            if isinstance(child, astroid.FunctionDef) and child.name == "__init__":
                # Check if 'detail' parameter is used (which is wrong for HTTPException subclasses)
                has_detail_param = False
                
                if child.args and child.args.args:
                    for arg in child.args.args:
                        if arg.name == "detail":
                            has_detail_param = True
                
                # If using 'detail' parameter, flag it (should use 'message' instead)
                if has_detail_param:
                    self.add_message("http-exception-init-must-accept-message", node=child)

    def _check_super_init_call(self, node: astroid.ClassDef):
        """Check that super().__init__() uses correct keyword argument."""
        # Skip BaseAppException itself
        if node.name == "BaseAppException":
            return
        
        inherits_httpexception = self._inherits_from_httpexception(node)

        # Look for __init__ method directly in this class (not nested)
        for child in node.body:
            if isinstance(child, astroid.FunctionDef) and child.name == "__init__":
                # Now look for super().__init__() calls within this __init__
                for call_node in child.nodes_of_class(astroid.Call):
                    # Check if this is a super().__init__() call
                    if isinstance(call_node.func, astroid.Attribute) and call_node.func.attrname == "__init__":
                        # Check if it's calling super()
                        if isinstance(call_node.func.expr, astroid.Call):
                            func_name = getattr(call_node.func.expr.func, 'name', None)
                            if func_name == 'super':
                                self._validate_super_init_keywords(call_node, inherits_httpexception, node.name)

    def _validate_super_init_keywords(self, call_node: astroid.Call, inherits_httpexception: bool, class_name: str):
        """Validate the keyword arguments in super().__init__() call."""
        if inherits_httpexception:
            # For HTTPException subclasses, both message= and detail= are allowed
            found_message = False
            found_detail = False
            
            for keyword in call_node.keywords:
                if keyword.arg == "message":
                    found_message = True
                elif keyword.arg == "detail":
                    found_detail = True
            
            # Both patterns are allowed, so no violations to check
            # message="literal" is allowed
            # detail=message is allowed
            # message=variable is allowed
            pass
        else:
            # For non-HTTPException subclasses, must use message=message
            has_message_with_message = False
            
            for keyword in call_node.keywords:
                if keyword.arg == "message":
                    if isinstance(keyword.value, astroid.Name) and keyword.value.name == "message":
                        has_message_with_message = True
                    else:
                        self.add_message("non-http-exception-use-message", node=call_node)
                        return
                elif keyword.arg == "detail":
                    # Non-HTTPException should not use detail
                    self.add_message("non-http-exception-use-message", node=call_node)
                    return
            
            if not has_message_with_message:
                self.add_message("non-http-exception-use-message", node=call_node)

    def _check_httpexception_raise(self, node: astroid.Raise):
        """Check that HTTPException raise statements use detail= keyword."""
        if node.exc is None:
            return
        
        # Check if raising HTTPException
        if isinstance(node.exc, astroid.Call):
            exc_name = None
            if isinstance(node.exc.func, astroid.Name):
                exc_name = node.exc.func.name
            elif isinstance(node.exc.func, astroid.Attribute):
                exc_name = node.exc.func.attrname
            
            if exc_name == "HTTPException":
                # Check if detail= keyword is used
                has_detail = False
                for keyword in node.exc.keywords:
                    if keyword.arg == "detail":
                        has_detail = True
                        break
                
                if not has_detail:
                    self.add_message("http-exception-raise-must-use-detail", node=node)

    # ---------------- Visitor overrides ----------------
    def visit_module(self, node: astroid.Module):
        """Module-level checks for exception files."""
        if not (self._is_exception_file(node) or self._is_baseapp_exception_file(node)):
            return

        # BaseAppException location
        if self._is_baseapp_exception_file(node):
            normalized_path = (node.file or "").replace("\\", "/")
            if not normalized_path.endswith("app/exception/baseapp_exception.py"):
                for class_node in node.nodes_of_class(astroid.ClassDef):
                    if class_node.name == "BaseAppException":
                        self.add_message("baseappexception-location-invalid", node=class_node)

        # Imports check
        if self._is_exception_file(node) and not self._is_baseapp_exception_file(node):
            for import_node in node.nodes_of_class(astroid.ImportFrom):
                if import_node.modname and "exception" in import_node.modname:
                    if not import_node.modname.endswith("baseapp_exception"):
                        self.add_message(
                            "exception-import-violation",
                            node=import_node,
                            args=(import_node.modname,),
                        )

    def visit_classdef(self, node: astroid.ClassDef):
        """Class-level checks for exception classes."""
        # Check ALL exception classes, not just in exception files
        # This ensures we catch exceptions defined anywhere
        is_exception_class = node.name.endswith("Exception")
        is_in_exception_file = self._is_exception_file(node) or self._is_baseapp_exception_file(node)
        
        # If it's an exception class OR in an exception file, perform checks
        if not (is_exception_class or is_in_exception_file):
            return

        # Naming convention
        if not node.name.endswith("Exception"):
            self.add_message("exception-name-must-end-with-exception", node=node, args=(node.name,))


        # Check __init__ signature for HTTPException subclasses
        if is_exception_class:
            self._check_init_signature(node)
        
        # Check super().__init__() calls
        if is_exception_class:
            self._check_super_init_call(node)

    def visit_raise(self, node: astroid.Raise):
        """Check raise statements for HTTPException usage."""
        pass
        # self._check_httpexception_raise(node)

    # def visit_assign(self, node: astroid.Assign):
    #     """Check for hardcoded HTTP status codes in exception files."""
    #     if not (self._is_exception_file(node) or self._is_baseapp_exception_file(node)):
    #         return

    #     if isinstance(node.value, astroid.Const) and isinstance(node.value.value, int):
    #         if 100 <= node.value.value <= 599:
    #             self.add_message("no-hardcoded-status-codes", node=node)


def register(linter: PyLinter):
    """Register the checker with pylint."""
    linter.register_checker(ExceptionAndKeywordChecker(linter))