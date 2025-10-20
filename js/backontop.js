
$(function(){
    $(".backontop").click(function(e){
      e.preventDefault();
      var target = $(this).attr("href");
      $('html,body').scrollTo(target.toString(),target.toString()); 
    });
  });