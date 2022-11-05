package auth;

//import org.springframework.stereotype.Component;
//import org.springframework.web.servlet.HandlerInterceptor;
//import org.springframework.web.servlet.ModelAndView;
//
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//
//@Component
//public class AuthInterceptor implements HandlerInterceptor {
//    @Override
//    public boolean preHandle(
//            HttpServletRequest request,
//            HttpServletResponse response,
//            Object handler
//    ) throws Exception {
//
//        String requestUrl = request.getRequestURI();
//        String authToken = request.getHeader("Authorization");
//
//        request.setAttribute("user", null);
//        return true;
//    }
//    @Override
//    public void postHandle(
//            HttpServletRequest request, HttpServletResponse response, Object handler,
//            ModelAndView modelAndView) throws Exception {
//
//    }
//
//    @Override
//    public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
//                                Object handler, Exception exception) throws Exception {}
//}
