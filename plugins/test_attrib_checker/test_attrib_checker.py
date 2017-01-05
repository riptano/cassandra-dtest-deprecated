from nose import plugins, selector


class _ClassCheckingSelector(selector.Selector):

    def wantClass(self, cls):
        default_result = super(_ClassCheckingSelector, self).wantClass(cls)

        if not default_result:
            falsy_test = hasattr(cls, '__test__') and not cls.__test__
            test_set_on_cls = '__test__' in cls.__dict__
            if falsy_test and not test_set_on_cls:
                raise RuntimeError(
                    "__test__ is falsey on {cls}'s superclass, but unset "
                    "on {cls}. The installed {module_name} module requires "
                    "__test__ to be explicitly set on classes removed from "
                    "discovery in this way.".format(
                        cls=cls.__name__, module_name=__name__
                    )
                )

        return default_result


class TestAttribChecker(plugins.Plugin):
    """
    Checks that any test classes skipped with `__test__ = False` are done so on
    the class itself.
    """

    enabled = True  # always use this plugin if it's installed
    name = 'test_attrib_checker'

    def configure(self, options, conf):
        """
        Noop conifigure method -- this plugin is always on.
        """
        pass

    def prepareTestLoader(self, loader):
        loader.selector = _ClassCheckingSelector(loader.config)
